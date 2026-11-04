import { UNITS } from './data.mjs';
import { groundRoute, groundLineClear } from './battle-navigation.mjs';
import { buildCastle, matchesCastleLayout, provisionLadders, ramHandle, ramApproach, wallHeightAt, battlementHeightAt } from './battle-siege.mjs';
import { battleGroundHeight, battlefieldClimate, fitSettlementTerrain, needsSettlementTerrainRepair, settlementApron, validSettlementTerrain } from './battle-terrain.mjs';

// This module has no DOM, clock or random global dependencies. The entire live
// simulation is JSON data, including attack phases, missiles and delayed armies.
export const BATTLE_VERSION = 1;
export const DEFAULT_ACTIVE_TROOPS = 300;
export const MAX_ACTIVE_TROOPS = 400;
export const BATTLE_SIZE_OPTIONS = Object.freeze([100, 150, 200, 300, MAX_ACTIVE_TROOPS]);
export const RESERVE_WAVE_SECONDS = 8;
const TAU = Math.PI * 2, STEP = 1 / 20;
const RULES_VERSION = 5, LEGACY_RAM_HP = 280;
const HERO_HEALTH = 1.2, HERO_WEAPON_DAMAGE = 1.1;
export const WALL_ACCURACY_BONUS = 10;
const PARRY_WINDOW = .3, PARRY_STUN = .7;
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
  spearman: { hp: 85, damage: 15, reach: 4, speed: 3.3, weapon: 'spear', shield: true, windup: .74, recovery: 1.06 },
  menAtArms: { hp: 100, damage: 25, reach: 2.1, speed: 3.3, weapon: 'sword', shield: true, windup: .3, recovery: .45 },
  heavyInfantry: { hp: 135, damage: 35, reach: 2.3, speed: 2.6, weapon: 'mace', shield: true, windup: .65, recovery: .9 },
  archer: { hp: 62, damage: 21, reach: 40, accuracy: 58, speed: 3.6, weapon: 'bow', ammo: 24, windup: .75, recovery: 1.4 },
  veteranArcher: { hp: 78, damage: 26, reach: 48, accuracy: 84, speed: 3.7, weapon: 'bow', ammo: 30, windup: .6, recovery: 1.1 },
  crossbow: { hp: 90, damage: 44, reach: 46, accuracy: 70, speed: 3, weapon: 'crossbow', ammo: 18, windup: .6, recovery: 3.1 },
  scout: { hp: 95, damage: 16, reach: 2.6, speed: 8, weapon: 'sword', windup: .32, recovery: .65 },
  lightCavalry: { hp: 110, damage: 23, reach: 2.8, speed: 8.2, weapon: 'sword', shield: true, windup: .35, recovery: .7 },
  cavalry: { hp: 145, damage: 31, reach: 4.1, speed: 7, weapon: 'lance', shield: true, windup: .4, recovery: .9 },
  knight: { hp: 180, damage: 40, reach: 4.4, speed: 6.5, weapon: 'lance', shield: true, windup: .4, recovery: .85 },
  ram: { hp: 2400, damage: 6, reach: 4, speed: 1.1, weapon: 'ram', windup: 1.7, recovery: 2.3 },
  catapult: { hp: 160, damage: 54, reach: 65, speed: 1.3, weapon: 'catapult', ammo: 16, windup: 2.1, recovery: 5.2 },
  trebuchet: { hp: 190, damage: 80, reach: 100, speed: .9, weapon: 'trebuchet', ammo: 12, windup: 3.2, recovery: 7.5 },
  siege: { hp: 160, damage: 48, reach: 60, speed: 1.3, weapon: 'catapult', ammo: 16, windup: 2.1, recovery: 5.5 }
};
export const TACTICAL_PROFILES = PROFILE;
function isPlayerHero(b,u){
  return u.id===b.commanderId && u.owner===b.playerOwner && UNITS[u.type].family!=='siege';
}
function troopMaxHp(b,u){
  const base=PROFILE[u.type].hp;
  return isPlayerHero(b,u)?Math.round(base*HERO_HEALTH):base;
}
function hasLegacyHealth(b,u){
  const version=b.battleRulesVersion||1;
  return (version<3&&u.type==='ram'&&u.maxHp===LEGACY_RAM_HP) ||
    (version<4&&isPlayerHero(b,u)&&u.maxHp===PROFILE[u.type].hp);
}
const WEAPONS = {
  sword: { damage: 24, reach: 2.1, windup: .3, recovery: .45 },
  spear: { damage: 14, reach: 4, windup: .76, recovery: 1.08 },
  axe: { damage: 30, reach: 2.3, windup: .6, recovery: .85 },
  mace: { damage: 32, reach: 2, windup: .65, recovery: .9 },
  bow: { damage: 23, reach: 44, windup: .7, recovery: 1.4 },
  crossbow: { damage: 44, reach: 46, windup: .6, recovery: 3.1 },
  lance: { damage: 31, reach: 4.1, windup: .4, recovery: .9 }
};
function wieldedWeapon(u) { return u.sidearm || u.weapon==='lance' && !u.mounted ? 'sword' : u.weapon; }
function profileFor(u) {
  const p=PROFILE[u.type], weapon=wieldedWeapon(u);
  return weapon===p.weapon || !WEAPONS[weapon] ? p : {...p,...WEAPONS[weapon]};
}
const MELEE_SWEEPS = {
  sword: {arc:.85,heavyArc:1.15,heavyTargets:3},
  axe: {arc:.72,heavyArc:.95,heavyTargets:2},
  mace: {arc:.55,heavyArc:.72,heavyTargets:1}
};
const footSpear = u => !u.mounted && !u.sidearm && u.weapon === 'spear';
function turnSpear(b, u, desired, dt) {
  const error=Math.atan2(Math.sin(desired-u.facing),Math.cos(desired-u.facing));
  const rate=u.action?.kind==='attack' ? .95 : u.braced ? 1.45 : 2.1;
  if (u.spearTurnAt!==b.time) { u.spearTurnAt=b.time; u.spearTurnUsed=0; }
  const remaining=Math.max(0,rate*dt-u.spearTurnUsed), turn=clamp(error,-remaining,remaining);
  u.facing+=turn; u.spearTurnUsed+=Math.abs(turn);
  if (u.action?.kind==='attack' && !u.action.impacted) u.action.facing=u.facing;
}
function faceHeading(b,u,heading,dt) {
  if (footSpear(u)) turnSpear(b,u,heading,dt);
  else u.facing=heading;
}

function inWeaponPath(u,v,action,reach) {
  const weapon=wieldedWeapon(u), dx=v.x-u.x, dz=v.z-u.z;
  if (weapon==='spear' || weapon==='lance') {
    const sin=Math.sin(action.facing), cos=Math.cos(action.facing);
    const ahead=dx*sin+dz*cos, lateral=Math.abs(dx*cos-dz*sin);
    return ahead>.1 && ahead<reach+.35 && lateral<(v.mounted?.79:.53)+(action.heavy?.08:0);
  }
  const sweep=MELEE_SWEEPS[weapon] || MELEE_SWEEPS.sword;
  return Math.hypot(dx,dz)<reach+.6 && angle(action.facing,Math.atan2(dx,dz)) < (action.heavy?sweep.heavyArc:sweep.arc);
}

function hash(value) { let n = 2166136261; for (const c of String(value)) n = Math.imul(n ^ c.charCodeAt(0), 16777619); return n >>> 0 || 1; }
export function rangedAccuracy(b, u) {
  const base = PROFILE[u.type]?.accuracy;
  if (base === undefined) return null;
  const drill = hash(`${b.seed}|${u.id}`) % 11 - 5;
  const posted = u.wallId && b.layout.walls.some(w => w.id === u.wallId && w.hp > 0);
  return Math.min(97, base + drill + (posted ? WALL_ACCURACY_BONUS : 0));
}
function random(b) { let x = b.rng; x ^= x << 13; x ^= x >>> 17; x ^= x << 5; b.rng = x >>> 0; return b.rng / 4294967296; }
function event(b, type, message, extra = {}) { b.nextEvent = (b.nextEvent || 0) + 1; b.events.push({ eventId: b.nextEvent, time: b.time, type, message, ...extra }); if (b.events.length > 100) b.events.shift(); }
function entryPoint(p) {
  let e = p.entryDirection ?? p.entry;
  if (typeof e === 'string') e = ({ north: { x: 0, z: -1 }, south: { x: 0, z: 1 }, east: { x: 1, z: 0 }, west: { x: -1, z: 0 }, northeast: { x: .7, z: -.7 }, northwest: { x: -.7, z: -.7 }, southeast: { x: .7, z: .7 }, southwest: { x: -.7, z: .7 } })[e.toLowerCase()];
  if (Number.isFinite(e)) e = { x: Math.sin(e), z: Math.cos(e) };
  const x = Number(e?.x ?? e?.dx) || 0;
  const rawZ = e?.z ?? e?.y ?? e?.dy;
  const z = rawZ !== undefined && Number.isFinite(Number(rawZ)) ? Number(rawZ) : (p.side ? 1 : -1);
  const norm = Math.max(Math.abs(x), Math.abs(z), .001);
  // Campaign approach bearings are not battlefield sides: the defender's
  // source tile can equal its destination and report the same bearing as its
  // attacker. Keep a lateral approach, but always enter the assigned end.
  return { x: clamp(x / norm * 55, -55, 55), z: p.side ? 75 : -75 };
}
function deploymentZonesFor(b) {
  const ward=b.layout.castle?.outer;
  return [
    { side: 0, minX: -66, maxX: 66, minZ: -72, maxZ: -44 },
    b.layout.fortified
      ? { side: 1, minX:(ward?.left ?? -35)+5, maxX:(ward?.right ?? 35)-5, minZ:(ward?.front ?? 12)+8, maxZ:(ward?.rear ?? 59)-5 }
      : { side: 1, minX: -66, maxX: 66, minZ: 44, maxZ: 72 }
  ];
}
function inDeploymentZone(zone, p) { return p.x >= zone.minX && p.x <= zone.maxX && p.z >= zone.minZ && p.z <= zone.maxZ; }
function troopClearance(u, v) { return u.family === 'siege' || v.family === 'siege' ? 2.6 : u.family === 'mounted' || v.family === 'mounted' ? 2.2 : 1.75; }
function freeTroopPosition(b, u, p, ignored = new Set(), placed = [], enemyGap = 0) {
  const y = p.y ?? terrainHeight(b, p.x, p.z);
  const blocks = v => v.id !== u.id && Math.abs(v.y - y) < 2.7 && distance(v, p) < (v.side !== u.side ? Math.max(enemyGap, troopClearance(u, v)) : troopClearance(u, v));
  return !obstruction(b, p.x, p.z, p.wallId ? { ...u, wallId: p.wallId, y } : null) && !b.units.some(v => alive(v) && !ignored.has(v.id) && blocks(v)) && !placed.some(blocks);
}
function deploymentPosition(b, u, preferred, ignored = new Set(), placed = []) {
  const zone = deploymentZonesFor(b)[u.side];
  const p = { x: clamp(preferred.x, zone.minX, zone.maxX), z: clamp(preferred.z, zone.minZ, zone.maxZ) };
  if (freeTroopPosition(b, u, p, ignored, placed)) return p;
  const candidates = [];
  for (let z = zone.minZ; z <= zone.maxZ; z += 2.8) for (let x = zone.minX; x <= zone.maxX; x += 2.8) candidates.push({ x, z });
  candidates.sort((a, c) => (a.x - p.x) ** 2 + (a.z - p.z) ** 2 - ((c.x - p.x) ** 2 + (c.z - p.z) ** 2));
  return candidates.find(point => freeTroopPosition(b, u, point, ignored, placed)) || null;
}
function entryPosition(b, u, a) {
  if (u.side === 1 && b.layout.fortified) {
    const spots = [];
    const ward=b.layout.castle?.inner || b.layout.castle?.outer || {left:-35,right:35,rear:59};
    for (let z=ward.rear-4; z>=ward.rear-16; z-=2.8) for (let x=ward.left+4; x<=ward.right-4; x+=2.8) spots.push({x,z});
    return spots.find(p => freeTroopPosition(b, u, p, new Set(), [], 6)) || null;
  }
  const candidates = [a.entry];
  for (let n = 1; n <= 55; n++) for (const sign of [1, -1]) candidates.push({ x: clamp(a.entry.x + n * 2.8 * sign, -74, 74), z: a.entry.z });
  return candidates.find(p => freeTroopPosition(b, u, p, new Set(), [], 6)) || null;
}
function layoutFor(b, tile) {
  const terrain = tile.terrain || 'plains';
  const layout = { width: 160, depth: 160, terrain, obstacles: [], walls: [], gate: null, ladders: [], stairs: [], objective: { x: 0, z: 38 }, bridges: [], navRevision: 0 };
  const initialWalls = Math.max(0, Number(tile.walls) || 0);
  layout.climate = battlefieldClimate(b.seed);
  const fortMax = tile.building === 'fort' ? [0, 45, 100, 180][clamp(tile.levels?.fort || tile.buildings?.fort || tile.level || tile.buildingLevel || 1, 1, 3)] : 0;
  const initialFortIntegrity = Math.max(0, Number(tile.fortIntegrity ?? fortMax) || 0);
  const baseline = tile.battleDamageIntegrity;
  const repaired = !!baseline && (initialWalls > baseline.walls + 1e-6 || initialFortIntegrity > baseline.fortIntegrity + 1e-6);
  const damageScale = baseline && !repaired ? clamp((initialWalls + initialFortIntegrity) / Math.max(.00001, baseline.walls + baseline.fortIntegrity), 0, 1) : 1;
  b.fortifications = { walls: initialWalls, fortIntegrity: initialFortIntegrity, initialWalls, initialFortIntegrity };
  buildCastle(layout,tile);
  const fortified = layout.castle?.enclosed;
  layout.fortified = !!fortified;
  if (fortified) {
    const wallMax = (tile.levels?.wall || tile.buildings?.wall || (initialWalls>0?1:0))*60;
    const condition = clamp((initialWalls+initialFortIntegrity)/Math.max(1,wallMax+fortMax),0,1);
    for (const w of [...layout.walls,layout.gate]) w.hp=w.maxHp*condition;
    if (condition < .3) Object.assign(layout.walls[0], { hp: 0, state: 'breached' });
    if (!condition) for (const w of [...layout.walls, layout.gate]) Object.assign(w, { hp: 0, state: 'breached' });
    for (const w of [...layout.walls, layout.gate]) {
      const saved = !repaired && tile.wallSections?.find(s => s.id === w.id);
      if (saved && Number.isFinite(saved.hp) && Number.isFinite(saved.maxHp) && saved.maxHp > 0) w.hp = w.maxHp * clamp(saved.hp / saved.maxHp, 0, 1) * damageScale;
      if (!repaired && tile.wallBreaches?.includes(w.id)) w.hp = 0;
      if (w.hp === 0 && saved?.breach && [saved.breach.x, saved.breach.z, saved.breach.width].every(Number.isFinite)) {
        w.breach={...saved.breach};
        // On a city upgrade, move the existing hole with its named section.
        if (!inside(w,w.breach.x,w.breach.z,1)) Object.assign(w.breach,{x:w.x,z:w.z});
      }
      w.state = w.hp <= 0 ? 'breached' : w.hp < w.maxHp * .7 ? 'damaged' : 'intact';
    }
    provisionLadders(layout);
    b.fortifications.initialTacticalHp = [...layout.walls, layout.gate].reduce((sum, w) => sum + w.hp, 0);
  }
  if (terrain === 'forest') {
    for (let i = 0; i < 42; i++) {
      const x = (random(b) - .5) * 140, z = (random(b) - .5) * 130;
      if (Math.abs(x) < 10 || layout.castle && Math.abs(x)<layout.castle.outer.right+4 && z>layout.castle.outer.front-5 && z<layout.castle.outer.rear+3) continue;
      layout.obstacles.push({ id: `tree-${i}`, kind: 'tree', x, z, radius: .9 + random(b) * .6, height: 7 + random(b) * 4 });
    }
  }
  if (terrain === 'mountain' || terrain === 'pass') {
    layout.obstacles.push({ id: 'mountain-west', kind: 'rock', x: -52, z: 0, width: 68, depth: 24, height: 13 }, { id: 'mountain-east', kind: 'rock', x: 52, z: 0, width: 68, depth: 24, height: 13 });
  }
  if (tile.river || tile.crossing || terrain === 'river') {
    const apron=settlementApron(layout);
    // On a fresh map place the whole watercourse and both crossings outside
    // the approaches. Saved battles retain their original banks and crossings.
    const z=apron ? Math.min(0,apron.front-12) : 0;
    if (z) layout.riverZ=z;
    layout.bridges = [{ x: -20, z, width: 12, depth: 12 }, { x: 28, z, width: 10, depth: 12 }];
    for (const [x, width] of [[-53, 42], [5, 34], [56, 46]]) layout.obstacles.push({ id: `water-${x}`, kind: 'water', x, z, width, depth: 10, height: .1 });
  }
  fitSettlementTerrain(layout);
  return layout;
}
export function terrainHeight(b, x, z) { return battleGroundHeight(b.layout, x, z); }
function inside(o, x, z, margin = 0) { return o.radius ? Math.hypot(o.x - x, o.z - z) < o.radius + margin : Math.abs(o.x - x) < o.width / 2 + margin && Math.abs(o.z - z) < o.depth / 2 + margin; }
function obstruction(b, x, z, u) {
  if (Math.abs(x) > 78.5 || Math.abs(z) > 78.5) return { id: 'boundary', kind: 'boundary' };
  for (const o of b.layout.obstacles) if (inside(o, x, z, .35)) return o;
  for (const w of b.layout.walls) {
    if (!wallHeightAt(w, x, z, .35)) continue;
    if (w.kind === 'gate' && u?.side === 1) continue;
    if (u?.wallId === w.id && u.y >= terrainHeight(b, x, z) + w.height - .3) continue;
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
function projectileCover(b, x, y, z) {
  const ground = terrainHeight(b,x,z);
  if (y <= ground) return 'ground';
  for (const w of b.layout.walls) {
    const h = battlementHeightAt(w,x,z);
    if (h && y <= terrainHeight(b,w.x,w.z) + h) return w;
  }
  const gate = b.layout.gate;
  if (gate?.hp > 0 && inside(gate,x,z) && y <= terrainHeight(b,gate.x,gate.z) + gate.height) return gate;
  return b.layout.obstacles.find(o => o.kind !== 'water' && inside(o,x,z) && y <= terrainHeight(b,o.x,o.z) + o.height);
}
function lineClear(b, a, c, u, projectile = false) {
  const length = distance(a, c), steps = Math.ceil(length / (projectile ? .36 : 1.2));
  const startY = projectile ? (a.y ?? terrainHeight(b,a.x,a.z)) + 1.6 : 0;
  const endY = projectile ? (c.y ?? terrainHeight(b,c.x,c.z)) + 1 : 0;
  for (let i = 1; i < steps; i++) {
    const t = i / steps, x = a.x + (c.x - a.x) * t, z = a.z + (c.z - a.z) * t;
    if (projectile) {
      const rayY = startY + (endY-startY) * t + Math.sin(t * Math.PI) * .22;
      if (projectileCover(b,x,rayY,z)) return false;
    } else if (obstruction(b,x,z,u)) return false;
  }
  return true;
}
function pathTo(b, u, target) {
  return groundRoute(b.layout,u,target,(x,z)=>obstruction(b,x,z,u));
}

function allocateCaps(b) {
  const totals = [0, 0]; for (const a of b.armies) totals[a.side] += count(a.starting);
  const total = totals[0] + totals[1];
  const cap0 = Math.min(totals[0], b.maxActive - (totals[1] ? 1 : 0), total ? Math.max(totals[0] ? 1 : 0, Math.round(b.maxActive * totals[0] / total)) : 0);
  b.activeCaps = [cap0, Math.min(totals[1], Math.max(totals[1] ? 1 : 0, b.maxActive - cap0))];
}
// Upgrade old saved battles without changing a soldier's identity, health,
// ammunition or casualties. Overflow soldiers wait for a later entry wave.
export function normalizeBattleCheckpoint(b) {
  if (!b || !Array.isArray(b.units) || !Array.isArray(b.armies)) return b;
  if ((b.battleRulesVersion || 1) < 5 && !b.auto && b.phase !== 'complete') {
    const commander = b.units.find(u => u.id === b.commanderId);
    if (commander && (commander.routing || commander.explicitOrder || commander.order.kind !== 'hold' || commander.fortPost || commander.wallPosition || commander.holdFire)) {
      commander.order = { kind: 'hold', x: commander.x, z: commander.z };
      commander.routing = false;
      commander.explicitOrder = false;
      commander.fortPost = null;
      commander.wallPosition = null;
      commander.path = [];
      commander.holdFire = false;
      const army = b.armies.find(a => a.armyId === commander.armyId);
      if (army?.retreating && ['active', 'reserve'].includes(commander.status)) army.retreating = false;
    }
  }
  for (const u of b.units) if (u.weapon==='lance' && !u.mounted) u.sidearm=true;
  if (needsSettlementTerrainRepair(b.layout)) {
    if (fitSettlementTerrain(b.layout)) b.layout.navRevision=(b.layout.navRevision || 0)+1;
    for (const u of b.units.filter(alive)) {
      if (['stairs','climb'].includes(u.action?.kind)) continue;
      const wall=u.wallId && b.layout.walls.find(w=>w.id===u.wallId && w.hp>0);
      u.y=terrainHeight(b,u.x,u.z)+(wall?.height||0);
    }
  }
  if ((b.battleRulesVersion || 1) < RULES_VERSION) for (const u of b.units) {
    if(hasLegacyHealth(b,u)){
      const maxHp=troopMaxHp(b,u);
      u.hp=u.hp/u.maxHp*maxHp;
      u.maxHp=maxHp;
    }
  }
  if (b.layout.ladders.some(l => !l.foot)) provisionLadders(b.layout);
  // Keep the original 200-slot migration for pre-wave saves. New battles may opt into larger armies.
  b.maxActive = clamp(Math.floor(b.maxActive || DEFAULT_ACTIVE_TROOPS), 4, b.battleRulesVersion>=2?MAX_ACTIVE_TROOPS:200);
  allocateCaps(b);
  b.nextReserveWave ??= b.time + RESERVE_WAVE_SECONDS;
  for (const side of [0, 1]) {
    const active = b.units.filter(u => alive(u) && u.side === side);
    if(active.length<=b.activeCaps[side])continue;
    const enemies=b.units.filter(u=>alive(u)&&u.side!==side);
    active.sort((a, c) => Number(c.id === b.commanderId) - Number(a.id === b.commanderId) || Math.min(Infinity, ...enemies.map(v => distance(a, v))) - Math.min(Infinity, ...enemies.map(v => distance(c, v))) || a.id.localeCompare(c.id));
    for (const u of active.slice(b.activeCaps[side])) {
      u.status = 'reserve'; u.wasDeployed = true; u.reserveQueuedAt = b.time;
      u.action = null; u.path = []; u.speed = 0; u.blocking = false; u.wallId = null;
    }
  }
  if (b.deploymentLayoutVersion !== 1) {
    for (const a of b.armies) a.entry = entryPoint(a);
    b.layout.deploymentZones = deploymentZonesFor(b);
    b.deploymentLayoutVersion = 1;
  }
  if (b.phase === 'deployment') repairDeployment(b);
  b.battleRulesVersion = RULES_VERSION;
  return b;
}
function validParapetPosition(b, u) {
  const wall = b.layout.walls.find(w => w.id === u.wallId && w.hp > 0);
  return u.side === 1 && wall && inside(wall, u.x, u.z, .1) && Math.abs(u.y - terrainHeight(b, u.x, u.z) - wall.height) < .3;
}
function repairDeployment(b) {
  const active = b.units.filter(alive), ignored = new Set(active.map(u => u.id)), retained = [], unsafe = [];
  for (const u of active) {
    const wall = validParapetPosition(b, u);
    const ownZone = inDeploymentZone(deploymentZonesFor(b)[u.side], u);
    if ((wall || ownZone) && freeTroopPosition(b, u, u, ignored, retained)) retained.push(u);
    else unsafe.push(u);
  }
  const repairIds = new Set(unsafe.map(u => u.id));
  for (const u of unsafe) {
    const p = deploymentPosition(b, u, u, repairIds, []);
    if (!p) { u.status = 'reserve'; u.action = null; continue; }
    u.x = p.x; u.z = p.z; u.y = terrainHeight(b, p.x, p.z); u.wallId = null; u.wallPosition = null;
    u.path = []; u.pathAt = -10; u.speed = 0;
    if (!u.order.lockFacing) u.facing = u.side ? Math.PI : 0;
    if (['hold', 'move', 'walls'].includes(u.order.kind)) u.order = { ...u.order, kind: 'hold', x: u.x, z: u.z, facing: u.order.lockFacing ? u.order.facing : u.facing };
    repairIds.delete(u.id);
  }
}
function initialPosition(b, u, a, index, entering = false) {
  if (entering) return entryPosition(b, u, a);
  const zone = deploymentZonesFor(b)[a.side], spacing = 2.8;
  const columns = Math.min(Math.floor((zone.maxX - zone.minX) / spacing) + 1, Math.max(12, Math.ceil(Math.sqrt(b.activeCaps[a.side]) * 2.4)));
  const row = Math.floor(index / columns) * spacing + (u.family === 'ranged' ? 5.6 : u.family === 'siege' ? 11.2 : 0);
  const preferred = { x: ((index % columns) - (columns - 1) / 2) * spacing, z: a.side ? zone.minZ + 2 + row : zone.maxZ - 2 - row };
  return deploymentPosition(b, u, preferred);
}
function deployReserves(b, initial = false) {
  for (const a of b.armies) if (!a.arrived && b.time >= a.arrivalSeconds) {
    a.arrived = true;
    a.entryPending = true;
    for (const u of b.units) if (u.side === a.side && alive(u)) u.morale = clamp(u.morale + 5, 0, 100);
    if (a.arrivalSeconds > 0) event(b, 'reinforcements', `${a.name || a.owner} reinforcements have arrived.`, { armyId: a.armyId, owner: a.owner, x: a.entry.x, z: a.entry.z });
  }
  const wave = initial || b.time + 1e-6 >= b.nextReserveWave;
  if (wave && !initial) b.nextReserveWave = b.time + RESERVE_WAVE_SECONDS;
  for (const side of [0, 1]) {
    const active = b.units.filter(u => u.side === side && alive(u));
    let remaining = b.activeCaps[side] - active.length;
    const eligible = b.armies.filter(a => a.side === side && a.arrived && !a.retreating && (wave || a.entryPending));
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
      const missingRole = b.activeCaps[side] >= groups.length ? groups.find(g => !g.blocked && g.reserves.length && !active.some(u => u.armyId === g.a.armyId && u.type === g.type)) : null;
      const group = commanderGroup || missingRole || groups.find(g => g.reserves.length && !g.blocked);
      const index = group.reserves.findIndex(u => u.id === b.commanderId), u = group.reserves.splice(index >= 0 ? index : 0, 1)[0];
      const p = initialPosition(b, u, group.a, initial ? active.length : 0, !initial);
      if (!p) { group.reserves.unshift(u); group.blocked = true; continue; }
      Object.assign(u, { status: 'active', x: p.x, z: p.z, y: terrainHeight(b, p.x, p.z), facing: side ? Math.PI : 0, arrived: true, deployedAt: b.time });
      u.wasDeployed = true;
      if (!u.explicitOrder) u.order = { kind: b.phase === 'deployment' ? 'hold' : 'advance', x: p.x, z: p.z, facing: u.facing };
      else if (u.order.anchorOnArrival) u.order = { ...u.order, x: p.x, z: p.z, facing: u.order.lockFacing ? u.order.facing : u.facing, wallId: null, anchorOnArrival: false };
      active.push(u);group.deployed++; remaining--;
    }
    for (const a of eligible) {
      const arrived = b.units.filter(u => u.armyId === a.armyId && u.deployedAt === b.time);
      if (arrived.length) {
        a.entryPending = false;
        if (!initial) event(b, 'reserveWave', `${arrived.length} troops enter from ${a.name || a.owner}'s approach.`, { armyId: a.armyId, count: arrived.length, x: a.entry.x, z: a.entry.z });
      }
    }
  }
}
export function createBattle(encounter, options = {}) {
  if (!encounter?.id || !Array.isArray(encounter.participants) || !encounter.participants.length) throw new Error('A recorded encounter with participating armies is required.');
  const ids = new Set();
  const b = { version: BATTLE_VERSION, battleRulesVersion: RULES_VERSION, encounterId: encounter.id, battleId: encounter.id, seed: encounter.seed ?? encounter.id, rng: hash(encounter.seed ?? encounter.id), time: 0, accumulator: 0, phase: 'deployment', playerOwner: encounter.playerOwner, playerSide: encounter.playerSide ?? 0, commanderId: null, units: [], armies: [], projectiles: [], events: [], winner: null, maxActive: clamp(Math.floor(options.maxActive || DEFAULT_ACTIVE_TROOPS), 4, MAX_ACTIVE_TROOPS), nextProjectile: 0, nextArmyPlan: 0, nextMorale: 1, nextReserves: 1, nextReserveWave: RESERVE_WAVE_SECONDS, occupation: 0, auto: false };
  b.layout = layoutFor(b, encounter.tile || {});
  b.layout.deploymentZones = deploymentZonesFor(b); b.deploymentLayoutVersion = 1;
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
    const u = roster.find(u=>u.type===options.commanderType&&u.family!=='siege') || roster.find(u => u.family !== 'siege') || roster[0];
    if (u) {
      b.commanderId=u.id;b.playerSide=u.side;u.commander=true;
      b.commanderName=commanderArmy.general?.name || commanderArmy.general || `${commanderArmy.name || commanderArmy.owner} Captain`;
      u.hp=u.maxHp=troopMaxHp(b,u);
    }
  }
  allocateCaps(b); deployReserves(b, true);
  if (b.layout.fortified) postDefenders(b, b.units.filter(u => fighting(u) && u.side === 1 && (u.owner !== b.playerOwner || options.autoGarrison)));
  event(b, 'deployment', 'Place your formations, then begin the battle.');
  return b;
}
export function beginBattle(b) {
  if (b.phase === 'deployment' || b.battleRulesVersion !== RULES_VERSION || b.maxActive > MAX_ACTIVE_TROOPS || b.deploymentLayoutVersion !== 1) normalizeBattleCheckpoint(b);
  if (b.phase !== 'deployment') return b;
  b.phase = 'combat';
  for (const u of b.units) if (alive(u) && !u.explicitOrder && !u.fortPost) u.order = { kind: 'advance' };
  event(b, 'begin', 'The battle has begun.');
  checkVictory(b);
  return b;
}

function selected(b, selection) {
  let candidates = b.units.filter(u => u.owner === b.playerOwner && u.id !== b.commanderId && u.status !== 'dead' && u.status !== 'escaped');
  if (Array.isArray(selection)) return candidates.filter(u => selection.includes(u.id) || selection.includes(u.armyId));
  if (selection && typeof selection === 'object') {
    if (selection.armyId) candidates = candidates.filter(u => u.armyId === selection.armyId);
    if (selection.ids) candidates = candidates.filter(u => selection.ids.includes(u.id));
    selection = selection.group || 'all';
  }
  if (!selection || selection === 'all') return candidates;
  if (selection === 'spearmen') return candidates.filter(u => u.type === 'spearman');
  if (selection === 'archers') return candidates.filter(u => ['archer', 'veteranArcher'].includes(u.type));
  if (selection === 'cavalry') return candidates.filter(u => u.family === 'mounted');
  if (['infantry', 'ranged', 'siege'].includes(selection)) return candidates.filter(u => u.family === selection);
  if (PROFILE[selection]) return candidates.filter(u => u.type === selection);
  return candidates.filter(u => u.armyId === selection || u.id === selection);
}
const FORMATIONS = new Set(['balanced', 'line', 'column', 'wedge', 'square', 'skirmish', 'defensive', 'spearWall', 'flanking', 'charge']);
function formationOffsets(units, formation) {
  const spacing = formation === 'skirmish' ? 3.6 : formation === 'defensive' || formation === 'spearWall' ? 1.8 : units.some(u => u.family === 'mounted' || u.family === 'siege') ? 2.8 : 2.2;
  const width = formation === 'line' || formation === 'spearWall' ? Math.min(20, units.length) : formation === 'column' ? Math.min(3, units.length) : Math.max(3, Math.ceil(Math.sqrt(units.length) * (formation === 'defensive' ? 2 : 1.4)));
  const points = units.map((u, i) => {
    if (formation === 'wedge') { const row = Math.floor(Math.sqrt(i)); return { x: (i - row * row - row) * spacing, z: -row * spacing }; }
    if (formation === 'square') {
      const edge = Math.max(1, Math.ceil(units.length / 4)), side = Math.floor(i / edge), t = i % edge, half = edge * spacing / 2;
      return [{ x: -half + t * spacing, z: half }, { x: half, z: half - t * spacing }, { x: half - t * spacing, z: -half }, { x: -half, z: -half + t * spacing }][side];
    }
    return { x: (i % width) * spacing, z: -Math.floor(i / width) * spacing };
  });
  const center = points.reduce((p, v) => ({ x: p.x + v.x / points.length, z: p.z + v.z / points.length }), { x: 0, z: 0 });
  return points.map(p => ({ x: p.x - center.x, z: p.z - center.z }));
}
const ORDER_ALIASES = { moveAndFace: 'move', holdPosition: 'hold', followMe: 'follow', attackFormation: 'attack', attackNearest: 'nearest', holdfire: 'holdFire' };
function disengage(u) {
  if (['shoot', 'attack', 'siege'].includes(u.action?.kind)) u.action = null;
  u.targetId = null; u.blocking = false; u.braced = false; u.chargeDistance = 0;
}
function recallRetreat(b, u) {
  if (!u.order.orderedRetreat) return;
  u.routing = false;
  const army = b.armies.find(a => a.armyId === u.armyId);
  if (army) army.retreating = false;
}
function followerDestination(b, u, commander) {
  if (!Number.isInteger(u.order.followSlot)) {
    const taken = new Set(b.units.filter(v => alive(v) && v.order.kind === 'follow').map(v => v.order.followSlot));
    let next = 0; while (taken.has(next)) next++;
    u.order.followSlot = next;
  }
  // Widen successive ranks so a large company fits behind the moving commander.
  let row = 0, width = 3, column = u.order.followSlot;
  while (column >= width) { column -= width; row++; width = Math.min(15, width + 2); }
  const side = Math.ceil(column / 2) * (column % 2 ? -1 : 1) * 2.7, back = 2.8 + row * 2.7;
  const cos = Math.cos(commander.facing), sin = Math.sin(commander.facing);
  const x = commander.x + cos * side - sin * back, z = commander.z - sin * side - cos * back;
  if (commander.wallId) {
    const w = b.layout.walls.find(w => w.id === commander.wallId && w.hp > 0);
    if (w) {
      const horizontal = w.width > w.depth, slots = Math.floor((horizontal ? w.width : w.depth) / 1.8);
      const along = horizontal ? commander.x - w.x : commander.z - w.z;
      const offsets = Array.from({length:slots}, (_,i)=>(i-(slots-1)/2)*1.8)
        .filter(offset=>Math.abs(offset-along)>1.2).sort((a,c)=>Math.abs(a-along)-Math.abs(c-along));
      const offset = offsets[u.order.followSlot];
      if (offset !== undefined) return {x:w.x + (horizontal ? offset : 0), z:w.z + (horizontal ? 0 : offset), wallId:w.id};
      // Once the parapet is full, form a second rank at its interior stair.
      const stair = b.layout.stairs.find(s=>s.wallId===w.id);
      if (stair) {
        const inward = 2 + row * 1.8;
        return nearestBattlePosition(b,{x:stair.x + (horizontal ? side : Math.sign(stair.x - w.x) * inward),
          z:stair.z + (horizontal ? Math.sign(stair.z - w.z) * inward : side)},u);
      }
    }
  }
  return nearestBattlePosition(b, { x, z }, u);
}
export function issueOrder(b, selection, order, params = {}) {
  if (b.phase === 'complete') return { accepted: false, reason: 'The battle has ended.' };
  order = ORDER_ALIASES[order] || order;
  if (!['move', 'face', 'hold', 'advance', 'charge', 'follow', 'attack', 'nearest', 'fire', 'holdFire', 'withdraw', 'retreat', 'formation', 'ladder', 'walls', 'crew', 'breach'].includes(order)) return { accepted: false, reason: 'Unknown battle order.' };
  const units = selected(b, selection).filter(u => order !== 'walls' || ['infantry', 'ranged'].includes(u.family) && (b.phase !== 'deployment' || u.side === 1));
  if (!units.length) return { accepted: false, reason: 'No available troops match the selection.' };
  if (order === 'ladder') return assignLadders(b, units, params.ladderId);
  if (order === 'crew' || order === 'breach') return assignRam(b, units, params.ramId, params.wallId);
  if (order === 'hold' && (params.atCurrent || params.holdCurrent)) {
    for (const u of units) {
      recallRetreat(b, u);
      u.order = { kind: 'hold', x: u.x, z: u.z, facing: Number.isFinite(params.facing) ? params.facing : u.facing, lockFacing: Number.isFinite(params.facing) };
      if (u.wallId) u.order.wallId = u.wallId;
      if (u.status === 'reserve') u.order.anchorOnArrival = true;
      u.explicitOrder = true; u.path = []; u.pathAt = -10; u.chargeDistance = 0;
      if (!u.mounted || b.phase === 'deployment') u.speed = 0;
    }
    event(b, 'order', `Hold current ground: ${units.length} troops.`);
    return { accepted: true, count: units.length };
  }
  if (params.formation && !FORMATIONS.has(params.formation)) return { accepted: false, reason: 'Unknown tactical formation.' };
  if (order === 'face' && !Number.isFinite(params.facing)) return { accepted: false, reason: 'Choose the direction the formation should face.' };
  if (order === 'walls') return issueWallOrder(b, units, params);
  if (['move', 'withdraw'].includes(order) && (!Number.isFinite(params.x) || !Number.isFinite(params.z))) return { accepted: false, reason: 'Choose a destination on the battlefield.' };
  const deployed = units.filter(alive), centerUnits = deployed.length ? deployed : units;
  const center = centerUnits.reduce((p, u) => ({ x: p.x + u.x / centerUnits.length, z: p.z + u.z / centerUnits.length }), { x: 0, z: 0 });
  const pos = nearestBattlePosition(b, { x: params.x ?? center.x, z: params.z ?? center.z }, units[0]);
  const f = Number.isFinite(params.facing) ? params.facing : units[0].facing;
  const formation = params.formation || units[0].formation || 'balanced';
  const slotUnits = deployed.length ? deployed : units.slice(0, b.activeCaps[b.playerSide]);
  const offsets = formationOffsets(slotUnits, formation), slotOrder = [...deployed, ...units.filter(u => !alive(u))];
  const occupied = new Set(order === 'follow' ? b.units.filter(u => u.order.kind === 'follow' && !units.includes(u) && alive(u)).map(u => u.order.followSlot) : []);
  let followSlot = 0;
  const deploymentPositions = new Map();
  if (b.phase === 'deployment' && ['move', 'hold', 'formation'].includes(order)) {
    const ignored = new Set(deployed.map(u => u.id)), placed = [];
    for (const u of deployed) {
      const { x, z } = offsets[slotOrder.indexOf(u) % offsets.length];
      const preferred = { x: pos.x + x * Math.cos(f) + z * Math.sin(f), z: pos.z - x * Math.sin(f) + z * Math.cos(f) };
      const safe = deploymentPosition(b, u, preferred, ignored, placed);
      if (!safe) return { accepted: false, reason: 'There is no clear position for that formation inside its deployment area.' };
      deploymentPositions.set(u.id, safe);
      placed.push({ ...u, x: safe.x, z: safe.z, y: terrainHeight(b, safe.x, safe.z) });
    }
  }
  units.forEach(u => {
    if (order === 'fire' || order === 'holdFire') {
      u.holdFire = order === 'holdFire';
      if (u.holdFire && u.action?.ranged && !u.action.impacted) u.action = null;
      return;
    }
    if (params.formation) u.formation = params.formation;
    if (order !== 'retreat') recallRetreat(b,u);
    if (order === 'face') {
      if (b.phase==='deployment' || !u.mounted && !footSpear(u)) u.facing=f;
      u.order={kind:'hold',x:u.x,z:u.z,facing:f,lockFacing:true}; u.explicitOrder=true; return;
    }
    const { x, z } = offsets[slotOrder.indexOf(u) % offsets.length];
    const destination = nearestBattlePosition(b, { x: pos.x + x * Math.cos(f) + z * Math.sin(f), z: pos.z - x * Math.sin(f) + z * Math.cos(f) }, u);
    u.order = { kind: order === 'formation' ? 'move' : order, x: destination.x, z: destination.z, facing: f, lockFacing: Number.isFinite(params.facing), targetArmyId: params.targetArmyId || null, targetId: params.targetId || null, offsetX: x, offsetZ: z };
    if (order === 'follow' && u.id !== b.commanderId && alive(u)) {
      while (occupied.has(followSlot)) followSlot++;
      u.order.followSlot = followSlot++;
    }
    if (order === 'advance') u.order.company = (b.nextEvent || 0) + 1;
    if (['follow', 'move', 'formation', 'withdraw', 'retreat', 'advance'].includes(order) && (u.id !== b.commanderId || order === 'retreat')) disengage(u);
    u.explicitOrder = true; u.path = []; u.pathAt = -10;
    if (order === 'retreat') { u.routing = true; u.order.orderedRetreat = true; }
    if (b.phase === 'deployment' && ['move', 'hold', 'formation'].includes(order) && alive(u)) {
      const safe = deploymentPositions.get(u.id);
      u.x = safe.x; u.z = safe.z; u.y = terrainHeight(b, u.x, u.z); u.facing = f;
      u.wallId = null; u.wallPosition = null;
      u.order.x = u.x; u.order.z = u.z;
      u.order.kind = 'hold';
    }
  });
  if (order === 'retreat') for (const a of b.armies) {
    if (b.units.filter(u => u.armyId === a.armyId && u.status !== 'dead' && u.status !== 'escaped').every(u => u.routing)) a.retreating = true;
  }
  const actualPosition = deploymentPositions.size
    ? [...deploymentPositions.values()].reduce((center, p) => ({ x: center.x + p.x / deploymentPositions.size, z: center.z + p.z / deploymentPositions.size }), { x: 0, z: 0 })
    : pos;
  event(b, 'order', `${order === 'holdFire' ? 'Hold fire' : order}: ${units.length} troops.`, { x: actualPosition.x, z: actualPosition.z });
  return { accepted: true, count: units.length, destination: actualPosition, reason: deploymentPositions.size && !inDeploymentZone(deploymentZonesFor(b)[units[0].side], pos) ? 'The formation stays inside its deployment area.' : pos.reason || null };
}
function assignLadders(b, units, ladderId, announce=true) {
  const ladders = usableLadders(b).filter(l => !ladderId || l.id === ladderId);
  const eligible = units.filter(u => u.side === 0 && ['ranged', 'infantry'].includes(u.family) && ladders.some(l=>l.wallId!==u.wallId && canApproachLayer(b,u,l.layer)));
  if (!ladders.length || !eligible.length) return { accepted: false, reason: 'Select foot troops and a ladder on an intact wall.' };
  const demand = new Map(ladders.map(l => [l.id, b.units.filter(u => fighting(u) && !eligible.includes(u) && u.order?.ladderId === l.id).length]));
  for (const u of eligible.sort((a, c) => a.id.localeCompare(c.id))) {
    const cost = l => distance(u, l.foot) + demand.get(l.id) * 3;
    const l = ladders.filter(l=>l.wallId!==u.wallId && canApproachLayer(b,u,l.layer)).reduce((best, next) => cost(best) <= cost(next) ? best : next);
    demand.set(l.id, demand.get(l.id) + 1);
    u.order = { kind: 'ladder', ladderId: l.id }; u.explicitOrder = true; u.routing = false;
    u.targetId = null; u.blocking = false; u.path = []; u.pathAt = -10;
    if (!['climb','stairs'].includes(u.action?.kind)) u.action = null;
  }
  if (announce) event(b, 'order', `${eligible.length} troops advancing to ${ladderId ? ladders[0].name : 'the ladders'}.`);
  return { accepted: true, count: eligible.length };
}
function usableLadders(b) {
  return b.layout.ladders.filter(l => b.layout.walls.some(w => w.id === l.wallId && w.hp > 0));
}
function ladderEntryBlocker(b,u,ladder) {
  if(u.action || u.stagger>0)return 'Finish current action';
  const occupied=b.units.some(v=>alive(v)&&v.action?.kind==='climb'&&v.action.ladderId===ladder.id&&(!v.action.ascending||v.action.progress<.23));
  return occupied?'Foot occupied':null;
}
export function playerLadderOption(b,u) {
  if(b.phase!=='combat'||!u||u.id!==b.commanderId||!fighting(u)||u.mounted||u.family==='siege'||u.wallId||['climb','stairs'].includes(u.action?.kind))return null;
  let ladder=null,range=2.5;
  for(const candidate of usableLadders(b)){
    if(!candidate.foot||candidate.active===false)continue;
    const d=distance(u,candidate.foot);
    if(d<range){range=d;ladder=candidate;}
  }
  return ladder?{ladder,blocked:ladderEntryBlocker(b,u,ladder)}:null;
}
export function playerStairOption(b, u) {
  if (b.phase !== 'combat' || !u || u.id !== b.commanderId || !fighting(u) || u.mounted || u.family === 'siege' || ['climb', 'stairs'].includes(u.action?.kind)) return null;
  let stair = null, range = 3;
  for (const candidate of b.layout.stairs || []) {
    if (u.wallId && u.wallId !== candidate.wallId || !b.layout.walls.some(w => w.id === candidate.wallId && w.hp > 0)) continue;
    const d = u.wallId ? Math.hypot(u.x - candidate.topX, u.z - candidate.topZ) : distance(u, candidate);
    if (d < range) { range = d; stair = candidate; }
  }
  const ascending = !u.wallId;
  return stair ? { stair, ascending, blocked: stairEntryBlocker(b, u, stair, ascending) } : null;
}
function inWard(u,ward) { return !!ward && u.x>=ward.left && u.x<=ward.right && u.z>=ward.front && u.z<=ward.rear; }
function canApproachLayer(b,u,layer) {
  return layer !== 'inner' || inWard(u,b.layout.castle?.outer) || u.wallId ||
    [...b.layout.walls,b.layout.gate].some(w=>w && w.layer!=='inner' && w.hp<=0);
}
function canPushRam(b,u,ram){
  return fighting(u)&&u.owner===ram.owner&&u.side===ram.side&&u.family==='infantry'&&!u.mounted&&!u.wallId&&
    !['climb','stairs'].includes(u.action?.kind)&&(u.id!==b.commanderId||b.auto);
}
function takeRamHandle(u,ram,handle,automatic){
  u.order={kind:'crew',ramId:ram.id,handle,automatic};u.explicitOrder=true;
  u.action=null;u.targetId=null;u.path=[];u.pathAt=-10;u.blocking=false;u.atHandle=false;
}
function aimRam(ram,wall){
  if(ram.ramTargetId!==wall.id)ram.action=null;
  ram.ramTargetId=wall.id;ram.order={kind:'breach',wallId:wall.id};ram.explicitOrder=true;
}
function assignRam(b, units, ramId, wallId) {
  const ram = b.units.find(u => u.id === ramId && u.type === 'ram' && u.side === 0 && fighting(u));
  const wall = [...b.layout.walls, b.layout.gate].find(w => w && w.id === wallId && w.hp > 0);
  if (!ram || !wall) return { accepted: false, reason: 'Choose an available ram and an intact exterior wall or gate.' };
  if (!canApproachLayer(b,ram,wall.layer)) return {accepted:false,reason:'Open the outer ward before hauling the ram to the inner defenses.'};
  if(!units.some(u=>u.owner===ram.owner))return {accepted:false,reason:'Choose a ram belonging to your army.'};
  const carriers=units.filter(u=>canPushRam(b,u,ram)).sort((a,c)=>distance(a,ram)-distance(c,ram)||a.id.localeCompare(c.id));
  aimRam(ram,wall);
  let assigned = 0;
  const chosen=carriers.slice(0,6);
  const handles=Array(6).fill(null);
  for(const u of b.units)if(fighting(u)&&u.order.kind==='crew'&&u.order.ramId===ramId&&Number.isInteger(u.order.handle))handles[u.order.handle]=u;
  for(const u of chosen){
    if(handles.includes(u)){u.order.automatic=false;assigned++;continue;}
    let handle=handles.indexOf(null);
    if(handle===-1)handle=handles.findIndex(v=>v.order.automatic&&!chosen.includes(v));
    if(handle===-1)handle=handles.findIndex(v=>!chosen.includes(v));
    if(handle===-1)continue;
    const replaced=handles[handle];
    if(replaced){replaced.order={kind:'advance'};replaced.explicitOrder=false;replaced.atHandle=false;}
    handles[handle]=u;takeRamHandle(u,ram,handle,false);assigned++;
  }
  return {accepted:true,count:assigned,reason:assigned?null:'Available infantry will fill the handles.'};
}
function issueWallOrder(b, units, params) {
  const walls = b.layout.walls.filter(w => w.kind!=='gate' && w.hp > 0 && (!params.wallId || w.id === params.wallId));
  if (!walls.length) return { accepted: false, reason: 'No intact wall section is available to defend.' };
  const selectedIds = new Set(units.map(u => u.id));
  const occupied = new Set(b.units.filter(u => u.wallPosition && u.order.kind === 'walls' && u.status !== 'dead' && !selectedIds.has(u.id)).map(u => u.wallPosition.slot));
  const slots = [];
  for (const w of walls) {
    const horizontal = w.width > w.depth, length = horizontal ? w.width : w.depth;
    const n = Math.max(1, Math.floor((length - 2) / 2));
    const f = w.id.includes('front') ? Math.PI : w.id.endsWith('wall-rear') ? 0 : w.id.endsWith('wall-west') ? -Math.PI / 2 : Math.PI / 2;
    for (let i = 0; i < n; i++) {
      const slot = `${w.id}:${i}`, offset = -length / 2 + 1.7 + i * 2;
      if (!occupied.has(slot)) slots.push({ slot, wallId: w.id, x: w.x + (horizontal ? offset : 0), z: w.z + (horizontal ? 0 : offset), facing: f });
    }
  }
  let assigned = 0; const placed = [];
  if (params.near) slots.sort((a,c) => distance(a,params.near) - distance(c,params.near));
  for (const u of units) {
    const slotIndex = slots.findIndex(s => {
      const wall = walls.find(w => w.id === s.wallId);
      return freeTroopPosition(b, u, { ...s, y: terrainHeight(b, s.x, s.z) + wall.height }, selectedIds, placed);
    });
    if (slotIndex < 0) continue;
    const [slot] = slots.splice(slotIndex, 1), wall = walls.find(w => w.id === slot.wallId);
    u.wallPosition = slot; u.order = { kind: 'walls', ...slot }; u.explicitOrder = true;
    u.path = []; u.pathAt = -10; u.routing = false;
    if (b.phase === 'deployment' && alive(u)) {
      Object.assign(u, { wallId: wall.id, x: slot.x, z: slot.z, y: terrainHeight(b, slot.x, slot.z) + wall.height, facing: slot.facing, speed: 0 });
    }
    placed.push({ ...u, x: slot.x, z: slot.z, y: terrainHeight(b, slot.x, slot.z) + wall.height });
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
  if (d > .05) { if (!keepFacing) faceHeading(b,u,Math.atan2(x-u.x,z-u.z),dt); u.x += (x - u.x) / d * step; u.z += (z - u.z) / d * step; }
  u.y = terrainHeight(b, u.x, u.z) + w.height; u.speed = d > .05 ? step / dt : 0;
}
function stairEntryBlocker(b, u, stair, ascending) {
  if (u.action || u.stagger > 0) return 'Finish current action';
  return b.units.some(v => v.id !== u.id && alive(v) && v.action?.kind === 'stairs' && v.action.stairId === stair.id && (v.action.ascending !== ascending || v.action.elapsed < 1.1)) ? 'Stairs occupied' : null;
}
function startStairs(b, u, stair, ascending) {
  if (u.mounted || stairEntryBlocker(b, u, stair, ascending)) return false;
  u.action = { kind: 'stairs', stairId: stair.id, wallId: stair.wallId, ascending, elapsed: 0, duration: 4, windup: 4, recovery: 0, fromX: u.x, fromZ: u.z, fromY: u.y, targetX: ascending ? stair.topX : stair.x, targetZ: ascending ? stair.topZ : stair.z };
  u.speed = 0; u.blocking = false; u.braced = false;
  return true;
}
function takeWallPosition(b, u, dt, keepFacing = false) {
  const slot = u.wallPosition, wall = slot && b.layout.walls.find(w => w.id === slot.wallId);
  if (!wall || wall.hp <= 0) { u.order = { kind: 'hold', x: u.x, z: u.z, facing: u.facing }; return false; }
  if (u.action?.kind === 'stairs' || u.action?.kind === 'climb') return true;
  if (u.wallId === wall.id) {
    if (distance(u, slot) > .3) { moveOnWall(b, u, slot, dt); return true; }
    if (!keepFacing) faceHeading(b,u,slot.facing,dt);
    u.speed = 0; return false;
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

const HORSE_BRAKING = 5;
function moveHorse(b,u,heading,targetSpeed,dt){
  // A rider may steer during a swing as well as during the AI movement pass.
  // Advance this horse's position exactly once per simulation step.
  if(u.horseStepAt===b.time||u.wallId||!u.mounted)return;
  u.horseStepAt=b.time;
  if(u.stagger>0){heading=u.facing;targetSpeed=0;}
  const speed=Math.max(0,u.speed||0),error=Math.atan2(Math.sin(heading-u.facing),Math.cos(heading-u.facing));
  const turnRate=Math.min(1.85,5.2/Math.max(1,speed));
  const turn=clamp(error,-turnRate*dt,turnRate*dt);
  // Tight reversals shed speed while the mount continues along its old bearing.
  const desired=Math.max(0,targetSpeed)*Math.max(0,Math.cos(Math.max(0,Math.abs(error)-.20)));
  const nextSpeed=speed+clamp(desired-speed,-HORSE_BRAKING*dt,2.8*dt);
  const meters=(speed+nextSpeed)*.5*dt,travel=u.facing+turn*.5;
  u.facing=Math.atan2(Math.sin(u.facing+turn),Math.cos(u.facing+turn));
  const lean=-clamp((turn/dt)*nextSpeed*.038,-.21,.21);
  u.horseLean=(u.horseLean||0)+clamp(lean-(u.horseLean||0),-dt,dt);
  u.speed=nextSpeed;
  const slices=Math.max(1,Math.ceil(meters/.32)),dx=Math.sin(travel)*meters/slices,dz=Math.cos(travel)*meters/slices;
  for(let i=0;i<slices&&meters>0;i++){
    const x=u.x+dx,z=u.z+dz;
    if(obstruction(b,x,z,u)){u.speed=0;u.chargeDistance=0;u.horseLean=0;u.pathAt=-10;break;}
    u.x=x;u.z=z;u.y=terrainHeight(b,x,z);
    u.rideDistance=(u.rideDistance||0)+meters/slices;
  }
  if(u.speed>4&&desired>4&&Math.abs(error)<.55)u.chargeDistance=(u.chargeDistance||0)+meters;
  else u.chargeDistance=Math.max(0,(u.chargeDistance||0)-dt*5);
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
    if (destination.wallId === u.wallId || options.direct) {
      moveOnWall(b, u, destination, dt, options.keepFacing); return;
    }
    const ward=wall.layer==='inner'?b.layout.castle?.inner:b.layout.castle?.outer;
    const outside = destination.z < (ward?.front ?? 12)-1 && u.side === 0;
    const closed = outside && [...b.layout.walls,b.layout.gate].filter(w=>w && w.layer===wall.layer).every(w => w.hp > 0);
    const ladder = closed && usableLadders(b).find(l => l.wallId === u.wallId);
    if (ladder && ['follow','retreat','move','withdraw'].includes(u.order.kind)) {
      if (distance(u,ladder.landing) > .65) moveOnWall(b,u,ladder.landing,dt);
      else if (!u.action && !b.units.some(v => alive(v) && v.action?.ladderId === ladder.id)) {
        u.action = { kind:'climb', ladderId:ladder.id, wallId:wall.id, progress:1, elapsed:5.8,
          duration:5.8, windup:4.8, recovery:1, ascending:false, phase:'crest' };
        u.wallId=null; u.speed=0;
      }
      return;
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
    if (b.time - u.pathAt > 3 || !u.pathTarget || distance(u.pathTarget, destination) > (u.order.kind === 'follow' ? .75 : 6) || u.navRevision !== b.layout.navRevision) {
      u.path = pathTo(b, u, destination); u.pathAt = b.time; u.pathTarget = { x: destination.x, z: destination.z }; u.navRevision = b.layout.navRevision;
    }
    while (u.path.length && distance(u, u.path[0]) < 1.3) {
      if (u.path.length>1 && !groundLineClear(b.layout,u,u.path[1],u.side)) break;
      u.path.shift();
    }
    target = u.path[0] || destination;
  }
  const dx = target.x - u.x, dz = target.z - u.z, d = Math.hypot(dx, dz);
  if (d < (u.path.length>1 ? .03 : .3)) {
    if(u.mounted)moveHorse(b,u,u.order.lockFacing?u.order.facing:u.facing,0,dt);
    else u.speed*=.5;
    return;
  }
  let speed = profile.speed * (options.speedScale ?? 1) * (options.sprint && u.stamina > 5 ? 1.45 : 1);
  if (u.family === 'mounted' && !u.mounted) speed = 3.3 * (options.sprint ? 1.45 : 1);
  if (u.family === 'mounted' && b.layout.terrain === 'forest') speed *= .55;
  if (u.formation === 'defensive' || u.formation === 'spearWall') speed *= .78;
  if (u.blocking) speed *= .58;
  if (u.action) speed *= .4;
  const slope = terrainHeight(b, target.x, target.z) - u.y;
  if (slope > 0) speed *= clamp(1 - slope * .06, .6, 1);
  if(u.mounted){
    if(!options.direct&&u.path.length<=1&&['hold','follow','move','withdraw','advance'].includes(u.order.kind))
      speed=Math.min(speed,Math.sqrt(2*HORSE_BRAKING*Math.max(0,d-.15)));
    moveHorse(b,u,Math.atan2(dx,dz),speed,dt);
    if(options.sprint&&u.speed>4)u.stamina=Math.max(0,u.stamina-dt*13);
    return;
  }
  u.speed = Math.min(speed, u.speed + dt * (u.family === 'mounted' ? 2.8 : 10));
  const step = Math.min(d, u.speed * dt), nx = u.x + dx / d * step, nz = u.z + dz / d * step;
  const blocked = obstruction(b, nx, nz, u);
  if (blocked) {
    u.chargeDistance = 0; u.speed = 0;
    if (options.direct && u.id !== b.commanderId && u.side === 0 && !u.wallId && !u.mounted && dz > 0) {
      const l = usableLadders(b).find(l => distance(u, l.foot) <= 2.3);
      if (l) startClimb(b, u, l, true);
    }
    return;
  }
  u.chargeDistance = Math.max(0, u.chargeDistance - dt * 3);
  if (!options.keepFacing) faceHeading(b,u,Math.atan2(dx,dz),dt);
  u.x = nx; u.z = nz; u.y = terrainHeight(b, nx, nz);
  if (options.sprint) u.stamina = Math.max(0, u.stamina - dt * 13);
}
function startClimb(b, u, l, personal = false) {
  if (u.mounted || u.family === 'siege' || ladderEntryBlocker(b,u,l)) return false;
  u.x = l.foot.x; u.z = l.foot.z; u.facing = 0; u.speed = 0; u.blocking = false;
  u.action = { kind: 'climb', ladderId: l.id, wallId: l.wallId, elapsed: 0, duration: 5.8, windup: 4.8, recovery: 1, progress: 0, phase: 'rungs', ascending: true, personal };
  event(b, 'ladder', `${l.name} in use.`, { x: l.x, z: l.z, unitId: u.id });
  return true;
}
// All soldiers in this step use the same queue order. Reserve and dead troops
// never occupy places, and the next step reflects recalls and new arrivals.
const ladderQueues = new WeakMap();
function ladderQueueIndex(b,l,u){
  let snapshot=ladderQueues.get(b);
  if(snapshot?.time!==b.time){
    snapshot={time:b.time,lanes:new Map()};ladderQueues.set(b,snapshot);
  }
  if(!snapshot.lanes.has(l.id)){
    const waiting=b.units.filter(v=>fighting(v)&&!v.wallId&&v.order.kind==='ladder'&&v.order.ladderId===l.id&&v.action?.kind!=='climb')
      .map(v=>({id:v.id,d:(v.x-l.foot.x)**2+(v.z-l.foot.z)**2})).sort((a,c)=>a.d-c.d||a.id.localeCompare(c.id));
    snapshot.lanes.set(l.id,new Map(waiting.map(({id},i)=>[id,i])));
  }
  return snapshot.lanes.get(l.id).get(u.id)??0;
}
function approachLadder(b, u, dt) {
  const l = usableLadders(b).find(l => l.id === u.order.ladderId);
  if (!l) { u.order = { kind: 'charge' }; u.pathAt = -10; return false; }
  if (u.wallId===l.wallId) { u.order = { kind: 'charge' }; return false; }
  if (u.wallId) { moveUnit(b,u,l.foot,dt); return true; }
  const index = ladderQueueIndex(b,l,u), dest = { x: l.foot.x + (index > 5 ? (index % 3 - 1) * 1.4 : 0), z: l.foot.z - Math.min(index, 5) * 1.4 - Math.floor(Math.max(index - 5, 0) / 3) * 1.5 };
  u.blocking = false; u.targetId = null;
  if (index === 0 && distance(u, l.foot) <= .85) { startClimb(b, u, l); u.speed = 0; }
  else moveUnit(b, u, dest, dt);
  return true;
}
function updateClimb(b, u, a, dt) {
  const l = b.layout.ladders.find(l => l.id === a.ladderId), w = b.layout.walls.find(w => w.id === l?.wallId);
  if (!l || !w || w.hp <= 0) {
    u.y = terrainHeight(b, u.x, u.z); u.action = null; u.wallId = null; u.pathAt = -10;
    if (u.order.kind === 'ladder') u.order = { kind: 'charge' };
    return;
  }
  // Checkpoints from the old single-climber route had no progress field.
  if (!Number.isFinite(a.progress)) Object.assign(a, { progress: clamp(a.elapsed / a.duration, 0, .95), ascending: true, phase: 'rungs', wallId: w.id });
  const assigned = u.order.kind === 'ladder' && u.order.ladderId === l.id;
  if ((!assigned && !a.personal || u.routing) && a.progress < .72) a.ascending = false;
  const others = b.units.filter(v => v !== u && alive(v) && v.action?.kind === 'climb' && v.action.ladderId === l.id);
  if (!a.ascending && others.some(v => v.action.progress < a.progress && v.order.kind === 'ladder' && !v.routing)) a.ascending = true;
  const ahead = others.filter(v => a.ascending ? v.action.progress > a.progress : v.action.progress < a.progress);
  let next = a.progress + (a.ascending ? 1 : -1) * dt / 5.8;
  for (const v of ahead) next = a.ascending ? Math.min(next, Math.max(a.progress, v.action.progress - .21)) : Math.max(next, Math.min(a.progress, v.action.progress + .21));
  if (a.ascending && next >= .76 && a.phase !== 'clear') {
    const defender = b.units.find(v => fighting(v) && v.side !== u.side && v.wallId === w.id && distance(v, l.landing) < 2.25);
    if (defender) {
      if (!l.held || b.time >= (l.nextHeldNotice || 0)) {
        event(b, 'ladderHeld', `${l.name} held by defenders`, { ladderId: l.id, x: l.x, z: l.z });
        l.nextHeldNotice = b.time + 8;
      }
      l.held = true;
      next = Math.min(next, Math.max(a.progress, .80));
      if (u.stagger<=0 && b.time >= (a.nextThrust || 0) && Math.abs(u.y - defender.y) <= 2 && distance(u, defender) <= profileFor(u).reach + .6) {
        u.facing = facing(u, defender); hit(b, u, defender, profileFor(u).damage * .8); a.nextThrust = b.time + 1.25;
      }
    } else l.held = false;
  }
  a.progress = clamp(next, 0, 1); a.elapsed = a.progress * a.duration;
  const rise = clamp(a.progress / .86, 0, 1);
  const lip = w.z - w.depth / 2 - .28, mantle = clamp((a.progress - .86) / .12, 0, 1);
  u.x = l.x; u.z = l.foot.z + (lip - l.foot.z) * rise + (l.landing.z-lip) * mantle;
  u.y = terrainHeight(b, u.x, u.z) + l.height * rise;
  if (!a.ascending && a.progress <= 0) { u.action = null; u.pathAt = -10; u.z -= .7; u.y = terrainHeight(b, u.x, u.z); return; }
  a.phase = a.progress < .76 ? 'rungs' : a.progress < .9 ? 'crest' : 'clear';
  if (a.phase !== 'clear' || !a.ascending) return;
  // Choose the closest free ledge, then step sideways so the next climber
  // never lands in a friendly. Claims live on actions and vanish on death.
  if (a.clearX === undefined) {
    const occupied = b.units.filter(v => v !== u && alive(v) && (v.wallId === w.id || v.action?.wallId === w.id && Number.isFinite(v.action.clearX)));
    for (let n = 1; n <= 14 && a.clearX === undefined; n++) for (const sign of [-1,1]) {
      const x = l.x + sign * n * 1.5;
      if (Math.abs(x - w.x) > w.width / 2 - .4 || occupied.some(v => Math.abs((v.action?.clearX ?? (v.order.assaultLanding ? v.order.x : v.x)) - x) < 1.2)) continue;
      a.clearX = x; break;
    }
  }
  if (a.clearX === undefined) { a.progress = .91; return; }
  const slide = Math.min(1, (a.progress - .9) * 10);
  // Only the short landing step belongs to the ladder. March farther along
  // the parapet in ordinary wall movement after the leading foot lands.
  u.x = l.x + Math.sign(a.clearX - l.x) * Math.min(1.4, Math.abs(a.clearX - l.x)) * slide;
  if (a.progress >= 1) {
    u.wallId = w.id; u.action = null; u.pathAt = -10; u.y = terrainHeight(b,u.x,u.z) + w.height;
    if (assigned) {
      u.order = { kind: 'hold', wallId: w.id, x: a.clearX, z: l.landing.z, facing: 0, assaultLanding: true, landedAt:b.time };
      event(b, 'landed', `${l.name} landing cleared.`, { unitId: u.id, wallId: w.id, x: u.x, z: u.z });
    }
  }
}
function nearestEnemy(b, u, candidates, max = Infinity) {
  let enemy = null, preferred = null, preferredSquared = Infinity;
  let bestSquared=max*max;
  const requested=u.order.targetId||u.targetId;
  const coverLimit=b.layout.terrain==='forest'&&u.family!=='mounted'?32*32:Infinity;
  for (const v of candidates) {
    if (!alive(v) || v.side === u.side || u.order.targetArmyId && v.armyId !== u.order.targetArmyId) continue;
    const dx=u.x-v.x, dz=u.z-v.z, squared=dx*dx+dz*dz;
    if(v.id===requested){preferred=v;preferredSquared=squared;}
    if(squared<bestSquared&&squared<coverLimit){enemy=v;bestSquared=squared;}
  }
  if(preferred&&Math.sqrt(preferredSquared)<Math.min(max,Math.sqrt(bestSquared)+(u.order.targetId?8:1.5)))return preferred;
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
  u.action = { kind, elapsed: 0, windup: ranged || u.family === 'siege' ? p.windup : p.windup * (heavy ? 1.25 : 1), recovery: p.recovery, duration: 0, heavy, targetId: target?.id || null, targetX: targetPoint.x, targetZ: targetPoint.z, targetY: Number.isFinite(targetPoint.y) ? targetPoint.y : terrainHeight(b, targetPoint.x, targetPoint.z), ranged, impacted: false, facing: u.facing };
  u.action.duration = u.action.windup + u.action.recovery;
  return true;
}
function kill(b, u, attacker) {
  if (!alive(u)) return;
  u.hp = 0; u.status = 'dead'; u.action = null; u.speed = 0; u.blocking = false;
  u.deathTime = b.time; u.deathFacing = u.facing;
  const direction = attacker ? facing(attacker, u) : u.facing;
  u.deathDirection = { x: Math.sin(direction), z: Math.cos(direction) };
  event(b, 'casualty', `${UNITS[u.type].name} fell.`, { unitId: u.id, armyId: u.armyId, x: u.x, y: u.y, z: u.z, facing: u.deathFacing });
  if (attacker) attacker.morale = clamp(attacker.morale + 2, 0, 100);
  for (const v of b.units) if (alive(v) && v.side === u.side && distance(v, u) < 12) v.morale = Math.max(0, v.morale - (u.commander ? 18 : 2.5));
  if (u.commander) event(b, 'commander', 'Your commander is incapacitated. You can continue commanding the surviving army.', { unitId: u.id });
}
function hit(b, attacker, target, raw, options = {}) {
  if (!alive(target)) return;
  let damage = isPlayerHero(b,attacker)?raw*HERO_WEAPON_DAMAGE:raw, blocked = false;
  const incoming = facing(target, attacker), frontal = angle(target.facing, incoming) < 1.08;
  if (options.spearThrust) {
    // A shaft crowded inside its point has little leverage. A presented horse
    // is a larger target for the same fully extended thrust.
    const extension=clamp((distance(attacker,target)-.7)/1.8,.35,1);
    damage*=extension*(target.mounted ? (frontal?2.15:1.55) : 1);
  }
  if (!options.projectile && !frontal && !attacker.mounted && attacker.family==='infantry' && footSpear(target)) damage*=1.2;
  const charge = attacker.mounted && attacker.speed > 4.5 && attacker.chargeDistance > 8 && !options.projectile;
  if (options.lanceThrust && !charge) damage*=.65;
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
    blocked = true;
    const axe = !options.projectile && wieldedWeapon(attacker) === 'axe' || options.heavy;
    target.stamina = Math.max(0, target.stamina - (axe ? 25 : 10));
    damage *= axe ? .48 : .14;
    target.blockedAt = b.time;
    event(b, 'block', 'An attack is blocked.', { x: target.x, z: target.z, unitId: target.id });
    if (!options.projectile && !target.parrySpent && Number.isFinite(target.blockStarted) && b.time - target.blockStarted < PARRY_WINDOW) {
      target.parrySpent = true; target.lastParryAt=b.time; target.counterUntil = b.time + 1.2;
      attacker.stagger = Math.max(attacker.stagger, PARRY_STUN); attacker.stunnedUntil=b.time+PARRY_STUN;
      attacker.speed=0; attacker.chargeDistance=0; attacker.blocking=false; attacker.braced=false;
      // Keep traversal actions intact when a climber's crest attack is parried.
      if (attacker.action?.kind === 'attack') attacker.action = null;
      event(b, 'parry', 'Timed block · attacker stunned.', { x:target.x, z:target.z, unitId:target.id, sourceId:attacker.id });
    }
  }
  const armor = UNITS[target.type].armor || (target.family === 'siege' ? .1 : 0);
  damage *= 1 - armor * (options.piercing || !options.projectile && wieldedWeapon(attacker) === 'mace' ? .35 : 1);
  if (options.projectile && target.wallId && target.y > attacker.y + 2 && attacker.family !== 'siege' && b.layout.walls.some(w => w.id === target.wallId && w.hp > 0)) damage *= .6;
  if (attacker.counterUntil > b.time) damage *= 1.25;
  if (!frontal && !options.projectile) target.morale = Math.max(0, target.morale - 2.5);
  if (['climb', 'stairs'].includes(target.action?.kind)) damage *= 1.4;
  const damageApplied = Math.min(target.hp, Math.max(1, damage)), direction = facing(attacker, target);
  target.hp -= damageApplied;
  event(b, 'hit', 'A weapon hits.', { x: target.x, y: target.y + (target.mounted ? 1.8 : 1.05), z: target.z, damage: Math.round(damageApplied), damageApplied, unitId: target.id, targetId: target.id, sourceId: attacker.id, targetType: target.type, targetFamily: target.family, directionX: Math.sin(direction), directionZ: Math.cos(direction), blocked });
  if (target.hp <= 0) kill(b, target, attacker);
  else if (options.spearThrust && frontal && distance(attacker,target)>1.85 && !(attacker.stunnedUntil>b.time)) {
    if (!blocked && target.mounted && target.speed>4.5 && target.chargeDistance>8) {
      target.speed=0; target.chargeDistance=0; target.stagger=Math.max(target.stagger,.45);
      event(b,'brace','A spear catches the charging horse.',{unitId:target.id,x:target.x,z:target.z});
    } else if (target.family==='infantry' && !target.wallId && !target.atHandle &&
        !['climb','stairs'].includes(target.action?.kind) && b.time>=(target.spearRecoilUntil || 0)) {
      const recoil=blocked?.2:.48, x=target.x+Math.sin(direction)*recoil,z=target.z+Math.cos(direction)*recoil;
      if (!obstruction(b,x,z,target)) {
        target.x=x; target.z=z; target.y=terrainHeight(b,x,z);
        target.speed*=.4; target.spearRecoilUntil=b.time+.25;
      }
    }
  }
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
  if (w.state === 'breached' && before !== 'breached') {
    if (w.kind!=='gate' && w.id !== 'gate' && u.type === 'ram') w.breach = { x:w.x, z:w.z, width:6 };
    b.layout.navRevision++;
    event(b, 'breach', `${w.id.endsWith('gate') ? 'The gate has fallen' : 'A wall is breached'}!`, { x: w.x, z: w.z, wallId: w.id });
    for (const l of b.layout.ladders.filter(l => l.wallId === w.id)) { l.active = false; l.held = false; }
    const at = ramApproach(w), roles=w.layer==='inner'?['innerReserve','innerGate']:['reserve','gate'];
    const reserves = b.units.filter(v => fighting(v) && roles.includes(v.fortPost) && !v.explicitOrder);
    for (const [i,v] of reserves.entries()) v.order = { kind:'hold', x:w.x + Math.sin(at.facing)*6 + Math.cos(at.facing)*(i%7-3)*2, z:w.z + Math.cos(at.facing)*6 - Math.sin(at.facing)*(i%7-3)*2, facing:at.facing + Math.PI };
    for (const v of b.units.filter(v => fighting(v) && v.side===0 && v.order.kind==='ladder' && v.action?.kind!=='climb')) {
      if (!b.layout.ladders.some(l => l.id===v.order.ladderId && l.wallId===w.id) && v.owner===b.playerOwner && !b.auto) continue;
      v.order={kind:'charge'}; v.pathAt=-10;
    }
  }
  updateFortifications(b);
}
function arrowAim(b, u, action) {
  const dx = action.targetX - u.x, dz = action.targetZ - u.z;
  const groundRange = Math.hypot(dx,dz), rise = action.targetY + 1 - (u.y + 1.6), range = Math.hypot(groundRange,rise);
  const centerShot = random(b) * 100 < rangedAccuracy(b,u);
  // Spread is angular: a poorly loosed shot can still connect at point-blank range.
  const radius = range / 30 * (centerShot ? .5 * Math.sqrt(random(b)) : 1.25 + 1.9 * random(b));
  const angle = random(b) * TAU, side = radius * Math.cos(angle), up = radius * Math.sin(angle);
  const fx = groundRange ? dx/groundRange : Math.sin(u.facing), fz = groundRange ? dz/groundRange : Math.cos(u.facing);
  const slope = rise / Math.max(range,.001);
  return {x:action.targetX + side*fz - up*slope*fx, y:action.targetY + 1 + up*groundRange/Math.max(range,.001),
    z:action.targetZ - side*fx - up*slope*fz};
}
function actionImpact(b, u, action) {
  if (u.type === 'ram') return;
  const p = profileFor(u), target = b.units.find(v => v.id === action.targetId), wall = [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])].find(w => w.id === action.targetId);
  if (action.ranged) {
    if (u.ammo <= 0) return;
    u.ammo--;
    const stone = u.family === 'siege';
    const aim = stone ? {x:action.targetX + (random(b)-.5)*1.8, z:action.targetZ + (random(b)-.5)*1.8, y:action.targetY+1} : arrowAim(b,u,action);
    if (stone && wall) aim.y = terrainHeight(b,aim.x,aim.z) + 3;
    b.projectiles.push({ id: `missile-${b.nextProjectile++}`, sourceId: u.id, side: u.side, kind: stone ? 'stone' : u.weapon === 'crossbow' ? 'bolt' : 'arrow', x: u.x, z: u.z, y: u.y + 1.6, startX: u.x, startZ: u.z, startY: u.y + 1.6, targetX: aim.x, targetZ: aim.z, targetY: aim.y, elapsed: 0, duration: Math.max(.15, distance(u, aim) / (stone ? 20 : 45)), damage: p.damage, wallId: wall?.id || null, splash: stone ? 4.8 : 0 });
    return;
  }
  if (wall && distance(u, wall) < p.reach + Math.max(wall.width, wall.depth) / 2) { hitWall(b, u, wall, u.type === 'ram' ? 38 : 3); return; }
  const weapon=wieldedWeapon(u), reach = p.reach;
  const maxTargets=action.heavy ? MELEE_SWEEPS[weapon]?.heavyTargets || 1 : 1;
  const targets = b.units.filter(v => alive(v) && v.side !== u.side && Math.abs(v.y - u.y) < 2.7 && inWeaponPath(u,v,action,reach) && lineClear(b, u, v, u)).sort((a, c) => distance(u, a) - distance(u, c));
  for (let i = 0; i < Math.min(maxTargets, targets.length); i++) {
    hit(b, u, targets[i], p.damage * (action.heavy ? 1.5 : 1) * Math.pow(.65, i), { heavy: action.heavy, spearThrust:weapon==='spear', lanceThrust:weapon==='lance' });
    if (u.stagger > 0) break;
  }
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
  if (a.kind === 'climb') { updateClimb(b, u, a, dt); return; }
  if (!a.impacted && a.elapsed >= a.windup) { a.impacted = true; actionImpact(b, u, a); }
  if (a.elapsed >= a.duration) u.action = null;
}
function updateProjectiles(b, dt) {
  for (const p of b.projectiles) {
    const ox = p.x, oy = p.y, oz = p.z;
    p.elapsed += dt; const t = clamp(p.elapsed / p.duration, 0, 1);
    p.x = p.startX + (p.targetX - p.startX) * t; p.z = p.startZ + (p.targetZ - p.startZ) * t;
    p.y = p.startY + (p.targetY - p.startY) * t + Math.sin(t * Math.PI) * (p.kind === 'stone' ? 16 : .22);
    const source = b.units.find(u => u.id === p.sourceId);
    if (!source) { p.done = true; continue; }
    let stoneImpact = null;
    if (p.kind === 'stone') {
      const steps=Math.max(1,Math.ceil(Math.hypot(p.x-ox,p.y-oy,p.z-oz)/.3));
      for(let i=1;i<=steps;i++) {
        const f=i/steps, x=ox+(p.x-ox)*f, y=oy+(p.y-oy)*f, z=oz+(p.z-oz)*f;
        stoneImpact=projectileCover(b,x,y,z);
        if (stoneImpact) { Object.assign(p,{x,y,z}); break; }
      }
    } else {
      const dx=p.x-ox, dy=p.y-oy, dz=p.z-oz, steps=Math.max(1,Math.ceil(Math.hypot(dx,dy,dz)/.23));
      const nearby=b.units.filter(u=>alive(u) && u!==source && Math.abs(u.x-ox)<Math.abs(dx)+.86 && Math.abs(u.z-oz)<Math.abs(dz)+.86);
      for(let i=1;i<=steps;i++){
        const f=i/steps, x=ox+dx*f, y=oy+dy*f, z=oz+dz*f;
        if(projectileCover(b,x,y,z)) { p.done=true; event(b,'missileBlocked','A missile strikes cover.',{x,y,z}); break; }
        const victim=nearby.find(u=>y >= u.y && y < u.y+(u.mounted?2.9:2.15) && Math.hypot(x-u.x,z-u.z)<.85);
        if (!victim) continue;
        if (victim.side !== source.side) hit(b,source,victim,p.damage,{projectile:true,piercing:p.kind==='bolt'});
        else event(b,'missileBlocked','An allied formation blocks the firing lane.',{x,y,z});
        p.done=true; break;
      }
    }
    if (t >= 1 || stoneImpact) {
      if (p.kind === 'stone') {
        for (const u of b.units) if (alive(u) && distance(p, u) < p.splash) hit(b, source, u, p.damage * (1 - distance(p, u) / (p.splash + 1)), { projectile: true, piercing: true });
        if (stoneImpact?.hp > 0) hitWall(b, source, stoneImpact, (UNITS[source.type].breach || 8) * 3);
        event(b, 'siegeImpact', 'A siege projectile impacts.', { x: p.x, z: p.z });
      }
      p.done = true;
    }
  }
  b.projectiles = b.projectiles.filter(p => !p.done);
}

function carryRam(b, u, dt) {
  const ram = b.units.find(v => v.id === u.order.ramId && fighting(v) && v.order.kind === 'breach' && v.ramTargetId);
  if (!ram) { u.order = { kind: 'hold', x: u.x, z: u.z }; u.atHandle = false; return; }
  if (u.order.handle === null) {
    const occupied = b.units.filter(v => fighting(v) && v.order.kind === 'crew' && v.order.ramId === ram.id);
    const open = [0,1,2,3,4,5].find(i => !occupied.some(v => v.order.handle === i));
    if (open !== undefined) u.order.handle = open;
    else { u.atHandle = false; if (distance(u, ram) > 6) moveUnit(b, u, ram, dt); return; }
  }
  const handle = ramHandle(ram, u.order.handle);
  u.blocking = false; u.targetId = null;
  if (distance(u, handle) > .65) { u.atHandle = false; moveUnit(b, u, handle, dt); }
  else { u.atHandle = true; u.x = handle.x; u.z = handle.z; u.y = terrainHeight(b, u.x, u.z); u.facing = ram.facing; u.speed = ram.speed; }
}
function updateRam(b, ram, dt) {
  const crew = b.units.filter(u => fighting(u) && u.order.kind === 'crew' && u.order.ramId === ram.id && u.atHandle && Number.isInteger(u.order.handle) && distance(u,ramHandle(ram,u.order.handle)) < .85);
  ram.crewCount = crew.length;
  const wall = [...b.layout.walls, b.layout.gate].find(w => w && w.id === ram.ramTargetId && w.hp > 0);
  if (!wall || ram.order.kind !== 'breach' || ram.routing) {
    ram.ramTargetId = null; ram.action = null; ram.speed = 0; ram.crewCount = 0;
    for (const u of b.units.filter(u => u.order.kind === 'crew' && u.order.ramId === ram.id)) {
      u.atHandle = false; u.order = { kind: wall ? 'hold' : 'charge', x: u.x, z: u.z }; u.explicitOrder = !!wall;
    }
    return;
  }
  if (crew.length < 4) { ram.speed = 0; ram.action = null; return; }
  const dest = ramApproach(wall), strength = crew.length / 6;
  if (distance(ram, dest) > .42) {
    const previous = {x:ram.x,z:ram.z};
    moveUnit(b, ram, dest, dt, { speedScale: strength, direct: groundLineClear(b.layout,ram,dest,ram.side) });
    ram.rolledDistance = (ram.rolledDistance || 0) + distance(ram,previous);
    ram.action = null;
  } else {
    ram.speed = 0; ram.facing = dest.facing;
    if (!ram.action || ram.action.kind !== 'ramStrike') {
      ram.action = { kind: 'ramStrike', targetId: wall.id, elapsed: 0, windup: 1.25 / strength, recovery: 1.8 / strength, duration: 3.05 / strength, impacted: false };
    }
    const a = ram.action; a.elapsed += dt;
    if (!a.impacted && a.elapsed + 1e-8 >= a.windup) {
      a.impacted = true;
      const material = wall.material==='timber' || wall.id === 'gate' ? 'wood' : 'stone';
      event(b, 'ramImpact', material === 'wood' ? 'The ram splinters the gate.' : 'The ram strikes masonry.', { unitId: ram.id, wallId: wall.id, material, x: ram.x + Math.sin(ram.facing) * 2.8, y: ram.y + 1.1, z: ram.z + Math.cos(ram.facing) * 2.8 });
      hitWall(b, ram, wall, (material === 'wood' ? 54 : 31) * strength);
    }
    if (a.elapsed >= a.duration) ram.action = null;
  }
  for (const u of crew) {
    const p = ramHandle(ram, u.order.handle);
    Object.assign(u, p, { y: terrainHeight(b, p.x, p.z), facing: ram.facing, speed: ram.speed });
  }
}
function postDefenders(b, troops) {
  const available = troops.filter(u => !u.explicitOrder && !u.fortPost);
  const ladders = usableLadders(b);
  const guards = available.filter(u => u.family === 'infantry' && u.type !== 'spearman');
  const marksmen = available.filter(u => u.family === 'ranged');
  const assign = (u,l,near) => {
    if (!u || !l) return;
    if (issueWallOrder(b,[u],{wallId:l.wallId,near}).accepted) u.fortPost = 'parapet';
  };
  for (const l of ladders) {
    const guard = b.units.find(u => fighting(u) && u.fortPost === 'parapet' && u.family === 'infantry' && u.wallPosition?.wallId === l.wallId);
    if (!guard) assign(guards.shift(),l,l.landing);
  }
  for (let i=0; i<marksmen.length && ladders.length; i++) {
    const sorted = ladders.slice().sort((a,c) => b.units.filter(v => fighting(v) && v.order.kind === 'walls' && v.wallPosition?.wallId === a.wallId).length - b.units.filter(v => fighting(v) && v.order.kind === 'walls' && v.wallPosition?.wallId === c.wallId).length);
    for (const l of sorted) { assign(marksmen[i], l); if (marksmen[i].fortPost) break; }
  }
  const posts = available.filter(u => !u.fortPost);
  for (const [i,u] of posts.entries()) {
    const gate = u.type === 'spearman', castle=b.layout.castle;
    const inner=castle?.inner && (gate ? i%2===0 : i%3!==0);
    const front=(inner?castle.inner.front:castle?.outer.front) ?? 12;
    const p = { x: (i % 7 - 3) * 2.2, z: front+(gate ? 8 : 13)+Math.floor(i / 7) % 3 * 2.4 };
    u.fortPost = inner ? (gate?'innerGate':'innerReserve') : gate ? 'gate' : 'reserve';
    u.order = {kind:'hold', ...p, facing:Math.PI};
    if (b.phase==='deployment') {
      const site=deploymentPosition(b,u,p);
      if (site) { Object.assign(u,site,{y:terrainHeight(b,site.x,site.z)}); Object.assign(u.order,site); }
    }
  }
}
function staffRams(b){
  if(!b.layout.fortified)return;
  const surfaces=[...b.layout.walls,b.layout.gate].filter(Boolean);
  const outerOpen=surfaces.some(w=>w.layer!=='inner' && w.hp<=0);
  const innerClosed=b.layout.castle?.inner && !surfaces.some(w=>w.layer==='inner' && w.hp<=0);
  const candidates=[];
  for(const ram of b.units.filter(u=>u.type==='ram'&&u.side===0&&fighting(u))){
    if(ram.explicitOrder&&ram.order.kind!=='breach')continue;
    let wall=surfaces.find(w=>w.id===ram.ramTargetId&&w.hp>0);
    if(!wall){
      if(outerOpen&&!innerClosed)continue;
      wall=outerOpen?surfaces.find(w=>w.id==='inner-gate'):ram.owner===b.playerOwner?b.layout.gate:
        surfaces.find(w=>w.id==='wall-east-front')||b.layout.gate;
      if(!wall||wall.hp<=0)continue;
      aimRam(ram,wall);
      event(b,'ramCrew',`Nearby infantry are crewing the ram for ${wall.id}.`,{unitId:ram.id,wallId:wall.id});
    }
    const crew=b.units.filter(u=>fighting(u)&&u.order.kind==='crew'&&u.order.ramId===ram.id);
    const occupied=new Set(crew.map(u=>u.order.handle));
    const free=[0,1,2,3,4,5].filter(i=>!occupied.has(i));
    // Older saves could contain explicitly assigned infantry waiting for a grip.
    const waiting=crew.filter(u=>u.order.handle===null).sort((a,c)=>distance(a,ram)-distance(c,ram));
    for(const u of waiting){if(!free.length)break;u.order.handle=free.shift();}
    if(!free.length)continue;
    for(const u of b.units){
      if(!canPushRam(b,u,ram)||u.order.kind==='crew')continue;
      // A player's recall, defense or ladder assignment takes precedence. AI foot
      // soldiers still approaching their ladder can reinforce their own ram.
      if(u.explicitOrder&&!(u.owner!==b.playerOwner&&u.aiAssault&&u.order.kind==='ladder'&&!u.action))continue;
      candidates.push({u,ram,free,d:(u.x-ram.x)**2+(u.z-ram.z)**2});
    }
  }
  // Match across the entire army so adjacent rams cannot claim the same soldier.
  candidates.sort((a,c)=>a.d-c.d||a.u.id.localeCompare(c.u.id)||a.ram.id.localeCompare(c.ram.id));
  for(const {u,ram,free} of candidates){
    if(!free.length||u.order.kind==='crew')continue;
    const slot=free.findIndex(handle=>{
      const point=ramHandle(ram,handle),end=pathTo(b,u,point).at(-1);
      return end&&end.x===point.x&&end.z===point.z;
    });
    if(slot>=0)takeRamHandle(u,ram,free.splice(slot,1)[0],true);
  }
}
function planSiege(b, a, troops) {
  if (!b.layout.fortified || a.owner === b.playerOwner && !b.auto) return;
  if (a.side === 1) { postDefenders(b,troops); return; }
  const surfaces=[...b.layout.walls,b.layout.gate].filter(Boolean);
  const outerOpen=surfaces.some(w=>w.layer!=='inner'&&w.hp<=0);
  const innerClosed=b.layout.castle?.inner&&!surfaces.some(w=>w.layer==='inner'&&w.hp<=0);
  if(outerOpen&&!innerClosed)return;
  const rams=troops.filter(u=>u.type==='ram');
  const ladders=usableLadders(b);
  for (const u of troops.filter(u=>u.family==='infantry' && (!u.explicitOrder || u.order.assaultLanding && u.order.cleared || u.aiAssault && u.order.kind==='charge'))) {
    const inner=b.layout.castle?.inner && innerClosed && (outerOpen || inWard(u,b.layout.castle.outer));
    if (inner && inWard(u,b.layout.castle.inner)) continue;
    const layerLadders=ladders.filter(l=>inner ? l.layer==='inner' : l.layer!=='inner');
    if (!layerLadders.length || !inner && u.wallId) continue;
    const cost=l=>distance(u,l.foot)+b.units.filter(v=>fighting(v) && v.order.ladderId===l.id).length*3;
    const ladder=rams.length?layerLadders[0]:layerLadders.reduce((a,l)=>cost(l)<cost(a)?l:a);
    if (assignLadders(b,[u],ladder.id,false).accepted) u.aiAssault=true;
  }
}
function planArmies(b) {
  const strengths = [0, 0]; for (const u of b.units) if (u.status === 'active' || u.status === 'reserve') strengths[u.side] += (UNITS[u.type].attack + UNITS[u.type].defense) * (u.hp / u.maxHp);
  for (const a of b.armies) {
    const troops = b.units.filter(u => u.armyId === a.armyId && fighting(u));
    if (!troops.length) continue;
    planSiege(b,a,troops);
    a.morale = troops.reduce((sum, u) => sum + u.morale, 0) / troops.length;
    const commander = a.general || {}, aggression = Number(commander.aggression ?? .5), defensive = strengths[a.side] < strengths[1 - a.side] * .7 || a.side === 1 && b.layout.fortified;
    for (const u of troops) {
      if (u.owner === b.playerOwner && !b.auto || u.explicitOrder || u.fortPost || u.allyRequestUntil > b.time) continue;
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
    if (u.type === 'ram') return null;
    return b.layout.walls.filter(w => w.hp > 0).sort((a, c) => distance(u, a) - distance(u, c))[0] || (b.layout.gate?.hp > 0 ? b.layout.gate : null);
  }
  return null;
}
function advanceCompanies(active) {
  const groups = new Map(), guides = new Map();
  for (const u of active) {
    if (u.order.kind !== 'advance' || !u.order.company || u.routing || u.type === 'ram') continue;
    if (!groups.has(u.order.company)) groups.set(u.order.company, []);
    groups.get(u.order.company).push(u);
  }
  for (const troops of groups.values()) {
    const center = troops.reduce((p, u) => ({x:p.x + u.x / troops.length, z:p.z + u.z / troops.length}), {x:0,z:0});
    let closest = null, range = Infinity;
    for (const v of active) if (v.side !== troops[0].side && fighting(v) && distance(center, v) < range) {
      closest = v; range = distance(center, v);
    }
    if (!closest) continue;
    const heading = facing(center, closest), sin = Math.sin(heading), cos = Math.cos(heading);
    for (const u of troops) {
      const {offsetX, offsetZ} = u.order;
      guides.set(u.id, {x:center.x + cos * offsetX + sin * (offsetZ + 4), z:center.z - sin * offsetX + cos * (offsetZ + 4)});
    }
  }
  return guides;
}
function aiUnit(b, u, active, dt, advanceGuide) {
  const a = b.armies.find(a => a.armyId === u.armyId);
  let p = profileFor(u);
  if (['stairs', 'climb'].includes(u.action?.kind)) return;
  if (!u.routing && u.order.kind==='hold' && u.order.lockFacing && footSpear(u)) turnSpear(b,u,u.order.facing,dt);
  if (u.order.kind === 'crew' && !u.routing) { carryRam(b,u,dt); return; }
  if (u.order.kind === 'ladder' && !u.routing && approachLadder(b,u,dt)) return;
  if (u.routing || u.order.kind === 'retreat') {
    u.routing = true; u.blocking = false; u.braced = false;
    const e = a.entry;
    moveUnit(b, u, e, dt, { sprint: u.family !== 'siege' });
    if (distance(u, e) < 5 || Math.abs(u.x) > 76 || Math.abs(u.z) > 76) { u.status = 'escaped'; u.action = null; event(b, 'escape', 'Routing troops reach an escape route.', { armyId: u.armyId }); }
    return;
  }
  if (u.order.kind === 'follow' && u.id !== b.commanderId) {
    const commander = b.units.find(v => v.id === b.commanderId && alive(v));
    if (commander) {
      const destination = followerDestination(b, u, commander);
      const changingLevel = (destination.wallId || null) !== (u.wallId || null);
      if (distance(u, destination) > .9 || changingLevel) {
        disengage(u);
        const stair = changingLevel && !u.wallId && b.layout.stairs?.find(s => s.wallId === destination.wallId);
        if (stair && distance(u, stair) < 1) startStairs(b, u, stair, true);
        else moveUnit(b, u, stair || destination, dt);
        return;
      }
    }
    if(!u.mounted)u.speed=0;
  }
  if (['move', 'withdraw'].includes(u.order.kind)) {
    moveUnit(b, u, u.order, dt, { keepFacing: u.order.kind === 'withdraw' });
    if (distance(u, u.order) >= .6) return;
    u.order.kind = 'hold';
    if(u.mounted)return;
    u.speed = 0;
    if (u.order.lockFacing) faceHeading(b,u,u.order.facing,dt);
  }
  // Keep the visible weapon swing committed through impact and recovery.
  // Re-targeting/moving every frame previously made fighters turn their backs
  // during a strike and caused the saved strike direction to miss the enemy.
  if (u.action && ['attack', 'shoot', 'siege'].includes(u.action.kind)) {
    if(!u.mounted)u.speed=Math.max(0,u.speed-dt*12);
    u.blocking = false; return;
  }
  let enemy = nearestEnemy(b, u, active);
  if (u.wallId && enemy) {
    const threats = active.filter(v => alive(v) && v.side !== u.side && Math.abs(v.y - u.y) < 2.7 && distance(u,v) < 3.4);
    if (threats.length) {enemy=nearestEnemy(b,u,threats);u.wallSightId=enemy?.id||null;}
    else if(u.family==='ranged'&&distance(u,enemy)<p.reach){
      // Scan in small batches. Stone still blocks every shot at release and in flight.
      if(b.time >= (u.nextWallScan||0)){
        u.nextWallScan=b.time+.3+(hash(u.id)%7)*.035;
        let visible=lineClear(b,u,enemy,u,true)?enemy:null;
        const others=[];
        if(!visible){
          const nearby=active.filter(v=>fighting(v)&&v.side!==u.side&&v!==enemy&&distance(u,v)<p.reach);
          const start=(u.wallScanCursor||0)%Math.max(1,nearby.length);
          for(let i=0;i<Math.min(8,nearby.length);i++)others.push(nearby[(start+i)%nearby.length]);
          u.wallScanCursor=start+others.length;
          visible=others.find(v=>lineClear(b,u,v,u,true));
        }
        u.wallSightId=visible?.id||null;
        if(!visible)others.unshift(enemy);
        if(!visible&&u.order.kind==='walls'&&u.wallPosition&&b.time>=(u.nextWallAngle||0)){
          u.nextWallAngle=b.time+2;
          const wall=b.layout.walls.find(w=>w.id===u.wallId), horizontal=wall.width>wall.depth;
          for (const shift of [-2,2,-4,4,-6,6]) {
            const point={x:u.wallPosition.x+(horizontal?shift:0),z:u.wallPosition.z+(horizontal?0:shift),y:u.y};
            if (!inside(wall,point.x,point.z,-.35) || active.some(v=>v!==u && v.wallId===wall.id && distance(v,point)<1.8)) continue;
            if (!others.some(v=>lineClear(b,point,v,u,true))) continue;
            Object.assign(u.wallPosition,{x:point.x,z:point.z,slot:`${wall.id}@${point.x},${point.z}`});
            Object.assign(u.order,u.wallPosition);
            break;
          }
        }
      }
      const sight=active.find(v=>v.id===u.wallSightId&&fighting(v));if(sight)enemy=sight;
    }
  }
  const d = enemy ? distance(u, enemy) : Infinity;
  if (u.order.assaultLanding && u.wallId) {
    if (!u.order.cleared && distance(u,u.order) > .25) { moveOnWall(b,u,u.order,dt); return; }
    u.order.cleared=true;
    const guard = active.find(v => fighting(v) && v.side !== u.side && v.wallId === u.wallId);
    if (guard && distance(u,guard)>p.reach) { moveOnWall(b,u,guard,dt); return; }
    if (!guard && b.time-u.order.landedAt>4 && enemy) {
      const queue = active.some(v=>fighting(v) && v.side===u.side && v.order.kind==='ladder' && b.layout.ladders.some(l=>l.id===v.order.ladderId && l.wallId===u.wallId));
      const ledge=b.layout.walls.find(w=>w.id===u.wallId), capacity=Math.min(13,Math.max(3,Math.floor(ledge.width/1.5)-2));
      const crowded = active.filter(v=>fighting(v) && v.side===u.side && v.wallId===u.wallId).length >= capacity;
      if (crowded || !queue && enemy.z>13) u.order={kind:'charge'};
    }
  }
  if (advanceGuide && d > Math.max(p.reach, 4)) {
    moveUnit(b, u, advanceGuide, dt); return;
  }
  if (u.order.kind === 'walls' && takeWallPosition(b, u, dt, footSpear(u) && d<p.reach+2)) return;
  u.targetId = enemy?.id || null;
  const oldBlock = u.blocking;
  u.blocking = !!enemy && d < 6 && u.shield && u.stamina > 14 && !u.action && (u.formation === 'defensive' || u.formation === 'spearWall' || enemy.action?.elapsed < enemy.action?.windup);
  if (u.blocking && !oldBlock) { u.blockStarted = b.time; u.parrySpent = false; }
  u.braced = footSpear(u) && u.speed < .7 && !u.action && ['spearWall', 'defensive'].includes(u.formation) && u.stamina > 15;
  const wall = targetWall(b, u);
  if (wall && (u.type === 'ram' || u.ammo > 0)) {
    const approach = { x: wall.x, z: wall.z - (u.type === 'ram' ? 3.5 : p.reach * .75) };
    if (distance(u, wall) <= p.reach + 2) { u.facing = facing(u, wall); startAttack(b, u, wall); u.speed *= .8; }
    else moveUnit(b, u, approach, dt);
    return;
  }
  if (!enemy) {
    if (b.securing && u.side === 0 && u.family !== 'siege') moveUnit(b, u, b.layout.objective, dt);
    else if(!u.mounted)u.speed*=.8;
    return;
  }
  const mayFaceEnemy = !u.order.lockFacing;
  const toward = facing(u,enemy);
  if (mayFaceEnemy) {
    if (u.mounted) {
      if (d < p.reach+.3) faceMountedTarget(b,u,toward,dt);
    } else faceHeading(b,u,toward,dt);
  }
  const canStrike = angle(u.facing,toward) < (footSpear(u) ? .28 : .65);
  const ranged = ['bow', 'crossbow'].includes(u.weapon) && u.ammo > 0 || u.family === 'siege' && u.type !== 'ram' && u.ammo > 0;
  const sidearm = u.weapon==='lance' && !u.mounted || ['bow','crossbow'].includes(u.weapon) && (u.ammo<=0 || d<3 && Math.abs(u.y-enemy.y)<2.7) || u.family==='siege' && u.type!=='ram' && u.ammo<=0;
  if (sidearm!==u.sidearm) { u.sidearm=sidearm; p=profileFor(u); }
  if (ranged && !u.sidearm) {
    if (d < 10 && !['hold', 'walls', 'follow'].includes(u.order.kind)) { moveUnit(b, u, { x: u.x + (u.x - enemy.x), z: u.z + (u.z - enemy.z) }, dt, { keepFacing: true }); u.facing = facing(u, enemy); }
    else if (canStrike && d < p.reach && !u.holdFire && (u.wallId&&u.family==='ranged'?u.wallSightId===enemy.id:lineClear(b, u, enemy, u, u.family === 'ranged'))) {
      const fired = startAttack(b, u, enemy);
      if (!fired && !u.action && u.stamina > 10 && !u.wallId && !['hold', 'walls', 'follow'].includes(u.order.kind)) {
        const direction = hash(u.id) % 2 ? 1 : -1;
        moveUnit(b, u, { x: u.x + Math.cos(u.facing) * direction * 3, z: u.z - Math.sin(u.facing) * direction * 3 }, dt, { keepFacing: true });
      } else u.speed *= .75;
    }
    else if (!['hold', 'walls', 'follow'].includes(u.order.kind)) moveUnit(b, u, enemy, dt, { keepFacing: mayFaceEnemy });
    return;
  }
  const reach = p.reach;
  // Begin setting the point as a closing opponent enters the windup, so a
  // full-speed approach is met near the tip rather than already at the hilt.
  const closing = footSpear(u) && angle(enemy.facing,facing(enemy,u))<.8 ? Math.min(1.65,enemy.speed*p.windup*.6) : 0;
  if (canStrike && d < reach + .3 + closing && Math.abs(u.y - enemy.y) < 2.7 && lineClear(b, u, enemy, u)) {
    startAttack(b, u, enemy, u.type === 'heavyInfantry' && random(b) < .2);
    if (!u.mounted) u.speed *= .8;
    return;
  }
  if (d < reach+.3 && !canStrike && (u.mounted || footSpear(u))) {
    if (!u.mounted) u.speed=Math.max(0,u.speed-dt*8);
    return;
  }
  if (u.order.kind === 'walls' || u.order.kind === 'follow') { if(!u.mounted)u.speed=0;return; }
  if (u.order.kind === 'hold' && u.wallId) { u.speed = 0; return; }
  if (u.order.kind === 'hold' && (u.explicitOrder || u.order.lockFacing || distance(enemy, u.order) > 8)) {
    moveUnit(b, u, u.order, dt, { keepFacing: true });
    if(u.order.lockFacing&&!u.mounted)faceHeading(b,u,u.order.facing,dt);
    return;
  }
  if (enemy.wallId && enemy.wallId !== u.wallId && u.family !== 'siege') {
    const wall = b.layout.walls.find(w => w.id === enemy.wallId), stair = b.layout.stairs?.find(s => s.wallId === enemy.wallId);
    const ladder = b.layout.ladders.find(l => wall && inside(wall, l.x, l.z, .5));
    if (ladder && u.z < ladder.z && u.side===0 && u.family === 'infantry' && canApproachLayer(b,u,ladder.layer)) { assignLadders(b,[u],ladder.id); approachLadder(b,u,dt); }
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
  moveUnit(b, u, destination, dt, { sprint: u.order.kind === 'charge' && d > 5, keepFacing: u.family !== 'mounted' && mayFaceEnemy });
}
function faceMountedTarget(b,u,heading,dt) {
  const committed=u.speed>4.5 && u.chargeDistance>8 && angle(u.facing,heading)<.5;
  moveHorse(b,u,heading,committed?u.speed:0,dt);
}
function trackAttackFacing(b, u) {
  const action = u.action;
  if (!action || action.impacted || !['attack', 'shoot', 'siege'].includes(action.kind) || u.order.lockFacing || u.routing || ['move', 'withdraw', 'retreat'].includes(u.order.kind)) return;
  const target = b.units.find(v => v.id === action.targetId && alive(v)) || [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])].find(w => w.id === action.targetId && w.hp > 0);
  if (!target) return;
  const heading=facing(u,target);
  if (u.mounted) { faceMountedTarget(b,u,heading,STEP); action.facing=u.facing; }
  else if (footSpear(u)) turnSpear(b,u,heading,STEP);
  else action.facing=u.facing=heading;
  action.targetX = target.x; action.targetZ = target.z; action.targetY = target.y ?? terrainHeight(b, target.x, target.z);
}
function preparePlayerGuard(b,u,input) {
  if (Number.isFinite(input.facing) && !u.mounted && !['stairs','climb'].includes(u.action?.kind)) {
    if (footSpear(u) || input.weapon==='spear' && u.weapon!=='spear' && !u.action && u.family!=='siege') turnSpear(b,u,input.facing,STEP);
    else u.facing=input.facing;
  }
  if (u.mounted && u.action?.kind==='attack' && !u.action.impacted) u.action.facing=u.facing;
  const held=!!input.block, pressed=input.blockPressed||held&&!u.guardHeld;
  if (pressed) { u.blockStarted=b.time; u.parrySpent=false; }
  u.guardHeld=held;
  u.blocking=!!(held||pressed)&&u.stamina>3&&!u.action&&u.stagger<=0;
}
function playerUnit(b, u, input, dt) {
  if (['stairs', 'climb'].includes(u.action?.kind)) return;
  if (input.weapon && !u.action && WEAPONS[input.weapon] && u.family !== 'siege') {
    const canEquip = input.weapon !== 'lance' || u.mounted;
    const hasQuiver = !['bow', 'crossbow'].includes(input.weapon) || u.family === 'ranged';
    if (canEquip && hasQuiver && u.weapon !== input.weapon) { u.weapon = input.weapon; u.sidearm = false; }
  }
  if (u.blocking) u.stamina = Math.max(0, u.stamina - dt * 5);
  if (!u.action && input.sidearm !== undefined) u.sidearm = !!input.sidearm;
  if (!u.mounted && u.weapon==='lance') u.sidearm=true;
  const mx = clamp(Number(input.moveX ?? input.move?.x) || 0, -1, 1), mz = clamp(Number(input.moveZ ?? input.move?.z) || 0, -1, 1), norm = Math.hypot(mx, mz);
  if (norm > .05) moveUnit(b, u, { x: u.x + mx / Math.max(1, norm) * 5, z: u.z + mz / Math.max(1, norm) * 5 }, dt, { direct: true, sprint: !!input.sprint, keepFacing: true });
  else if(u.mounted)moveHorse(b,u,(input.attack||u.blocking)&&Number.isFinite(input.facing)?input.facing:u.facing,0,dt);
  else u.speed = Math.max(0, u.speed - dt * 8);
  if (!u.wallId && (u.side === 0 ? u.z < -76 && mz < 0 : u.z > 76 && mz > 0)) {
    u.status = 'escaped'; u.action = null; u.speed = 0;
    event(b, 'escape', 'You leave the battlefield through your rear edge.', { armyId: u.armyId, unitId: u.id });
    return;
  }
  u.braced = footSpear(u) && u.blocking && u.speed < .5;
  if (input.attack && !u.blocking) {
    const enemy = nearestEnemy(b, u, b.units.filter(v => angle(u.facing, facing(u, v)) < 1));
    const reach = profileFor(u).reach;
    const point = { x: Number.isFinite(input.aimX) ? input.aimX : u.x + Math.sin(u.facing) * reach, z: Number.isFinite(input.aimZ) ? input.aimZ : u.z + Math.cos(u.facing) * reach };
    const aimDistance = distance(u, point);
    if (aimDistance > reach) { point.x = u.x + (point.x - u.x) / aimDistance * reach; point.z = u.z + (point.z - u.z) / aimDistance * reach; }
    if (enemy && distance(u, enemy) <= reach && angle(u.facing, facing(u, enemy)) < .12 && ['bow', 'crossbow'].includes(u.weapon)) Object.assign(point, { x: enemy.x, z: enemy.z, y: enemy.y });
    startAttack(b, u, enemy && distance(u, enemy) <= reach ? enemy : null, !!input.heavy, point);
  }
  if (input.climbLadderId || input.stairId || input.interact && !u.interactHeld) {
    const option = playerLadderOption(b,u);
    const stairs = playerStairOption(b,u);
    if (input.stairId) {
      if (stairs && !stairs.blocked && stairs.stair.id === input.stairId) startStairs(b, u, stairs.stair, stairs.ascending);
    } else if (option || input.climbLadderId) {
      if (option && !option.blocked && (!input.climbLadderId || option.ladder.id===input.climbLadderId)) startClimb(b,u,option.ladder,true);
    } else if (stairs) {
      if (!stairs.blocked) startStairs(b, u, stairs.stair, stairs.ascending);
    }
    else if (u.family === 'mounted' && !u.wallId&&!u.action) {
      u.mounted=!u.mounted;u.speed=0;u.horseLean=0;u.chargeDistance=0;
      if (u.weapon==='lance') u.sidearm=!u.mounted;
      event(b,'mount',u.mounted?'You mount your horse.':'You dismount. Your mount stays with you.');
    }
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
function separateUnits(b, active, dt, controlledId) {
  // Only nearby ground bodies can collide. Sweep by their initial x coordinate;
  // the small extra margin covers nudges from preceding pairs in this step.
  const bodies=active.filter(u=>u.type!=='ram'&&!u.wallId&&!(u.order.kind==='crew'&&u.atHandle)&&!['climb','stairs'].includes(u.action?.kind))
    .map(u=>({u,startX:u.x})).sort((a,c)=>a.startX-c.startX);
  for(let i=0;i<bodies.length;i++)for(let j=i+1;j<bodies.length;j++){
    if(bodies[j].startX-bodies[i].startX>2.6)break;
    const u=bodies[i].u,v=bodies[j].u;
    if(Math.abs(u.y-v.y)>2.7)continue;
    const dx=v.x-u.x,dz=v.z-u.z,radius=u.family==='siege'||v.family==='siege'?2.2:u.family==='mounted'||v.family==='mounted'?1.7:1.15;
    const squared=dx*dx+dz*dz;
    if(squared>=radius*radius)continue;
    const d=Math.sqrt(squared);
    if (u.family === 'mounted' && u.speed > 4 && u.side === v.side) { u.speed *= .7; u.chargeDistance = 0; }
    const push = Math.min((radius - d) * .5, dt * 2), direction = d > .0001 ? Math.atan2(dx, dz) : hash(`${u.id}:${v.id}`) / 4294967296 * TAU;
    const scale = u.id === controlledId || v.id === controlledId ? 2 : 1;
    const x = Math.sin(direction) * push * scale, z = Math.cos(direction) * push * scale;
    if (u.id !== controlledId && !obstruction(b, u.x - x, u.z - z, u)) { u.x -= x; u.z -= z; }
    if (v.id !== controlledId && !obstruction(b, v.x + x, v.z + z, v)) { v.x += x; v.z += z; }
  }
}
function canSecureKeep(b, u) {
  if (u.family === 'siege') return false;
  const objective=b.layout.objective;
  if (distance(u,objective)<4 && !u.wallId && !u.action?.ladderId) return true;
  const reaches = (from,to) => {
    const route = pathTo(b,{...u,...from,wallId:null},to);
    return route.length && distance(route[route.length-1],to)<.1;
  };
  const heldWall=u.wallId || (u.action?.kind==='climb' ? u.action.wallId : null);
  const from=heldWall ? b.layout.stairs.find(s=>s.wallId===heldWall) : u;
  if (!from) return false;
  const points=[from], visited=new Set(), ladders=u.mounted?[]:usableLadders(b);
  for (let i=0;i<points.length;i++) {
    const point=points[i];
    if (reaches(point,objective)) return true;
    for (const l of ladders) {
      if (visited.has(l.id) || !reaches(point,l.foot)) continue;
      const stair=b.layout.stairs.find(s=>s.wallId===l.wallId);
      visited.add(l.id);
      if (stair) points.push(stair);
    }
  }
  return false;
}
function checkVictory(b) {
  if (b.phase !== 'combat') return;
  const resistance = [0, 0];
  for (const u of b.units) if (u.type !== 'ram' && (fighting(u) || u.status === 'reserve' && !u.routing && !b.armies.find(a => a.armyId === u.armyId)?.retreating)) resistance[u.side]++;
  if (resistance[0] && resistance[1]) return;
  const winner = resistance[0] ? 0 : resistance[1] ? 1 : null;
  if (winner === 0 && b.layout.fortified) {
    // Once organized resistance ends, the field cap must not strand the
    // arriving foot company behind a full front line of mounted survivors.
    let occupier=b.units.find(u=>fighting(u) && u.side===0 && canSecureKeep(b,u));
    if (!occupier) {
      const reserves=b.units.filter(u=>u.side===0 && u.status==='reserve' && !u.routing && u.family!=='siege' && !b.armies.find(a=>a.armyId===u.armyId).retreating);
      reserves.sort((a,c) => b.armies.find(army=>army.armyId===a.armyId).arrivalSeconds - b.armies.find(army=>army.armyId===c.armyId).arrivalSeconds);
      const incoming=reserves.find(u=>canSecureKeep(b,{...u,...b.armies.find(a=>a.armyId===u.armyId).entry}));
      if (incoming) {
        const army=b.armies.find(a=>a.armyId===incoming.armyId);
        if (!army.arrived) return;
        incoming.wasDeployed=true; incoming.arrived=true;
        occupier=incoming;
      }
    }
    if (occupier) {
      b.securedBy=occupier.id;
      event(b,'occupied','The survivors secure the keep.',{unitId:occupier.id,x:b.layout.objective.x,z:b.layout.objective.z});
    }
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
  if(b.time>=(b.nextRamCrew??0)){staffRams(b);b.nextRamCrew=b.time+1;}
  if (b.time >= b.nextArmyPlan) { planArmies(b); b.nextArmyPlan = b.time + 4; }
  if (b.time >= b.nextMorale) { updateMorale(b); b.nextMorale = b.time + 1; }
  const active = b.units.filter(alive);
  // Apply the player's guard before either side's weapons reach their impact frame.
  const player=active.find(u=>u.id===b.commanderId);
  if(player&&!b.auto&&!input.commanderAI&&!player.routing)preparePlayerGuard(b,player,input);
  const guides = advanceCompanies(active);
  for (const u of active) if (!u.wallId && !['stairs','climb'].includes(u.action?.kind)) u.y=terrainHeight(b,u.x,u.z);
  for (const u of active) {
    if (!alive(u) || u.type==='ram') continue;
    if (u.order.kind !== 'crew') u.atHandle = false;
    if (u.wallId && !b.layout.walls.some(w => w.id === u.wallId && w.hp > 0)) {
      u.fortPost = null; u.wallPosition=null;
      if (u.order.kind === 'walls') { u.order={kind:'hold',x:u.x,z:u.z}; u.explicitOrder=false; }
      u.wallId = null; u.y = terrainHeight(b, u.x, u.z); u.hp -= 18; u.stagger = .8; u.action = null;
      if (obstruction(b,u.x,u.z,u)) {
        const p=nearestBattlePosition(b,u,u); u.x=p.x; u.z=p.z; u.y=terrainHeight(b,p.x,p.z);
      }
      if (u.hp <= 0) { kill(b, u, null); continue; }
    }
    u.stagger = Math.max(0, u.stagger - dt); u.attackCooldown = Math.max(0, u.attackCooldown - dt);
    if (u.stunnedUntil>b.time) continue;
    if (!u.action && !u.blocking) u.stamina = Math.min(100, u.stamina + dt * 11);
    if (u.id !== b.commanderId || b.auto || input.commanderAI || u.routing) {
      // An already-reformed follower may be defending its slot when the commander turns away.
      if (u.action && u.order.kind === 'follow' && !['climb', 'stairs'].includes(u.action.kind) && u.id !== b.commanderId) {
        const commander = b.units.find(v => v.id === b.commanderId && alive(v));
        if (commander && distance(u, followerDestination(b, u, commander)) > .9) disengage(u);
      }
      trackAttackFacing(b, u);
    }
    updateAction(b, u, dt);
    if (!alive(u)) continue;
    if (u.id === b.commanderId && !b.auto && !input.commanderAI && !u.routing) playerUnit(b, u, input, dt);
    else aiUnit(b, u, active, dt, guides.get(u.id));
    if(u.mounted&&alive(u)&&u.speed>0&&u.horseStepAt!==b.time)moveHorse(b,u,u.facing,0,dt);
  }
  separateUnits(b, active.filter(alive), dt, !b.auto && !input.commanderAI ? b.commanderId : null); updateProjectiles(b, dt);
  for (const ram of active) if (ram.type==='ram' && alive(ram)) updateRam(b,ram,dt);
  for (const l of b.layout.ladders) if (l.held && !active.some(u => fighting(u) && u.action?.ladderId===l.id && u.action.progress>=.75)) l.held=false;
  checkVictory(b);
}
export function stepBattle(b, dt, input = {}) {
  if (b.battleRulesVersion !== RULES_VERSION || b.maxActive > MAX_ACTIVE_TROOPS || b.deploymentLayoutVersion !== 1 || b.layout.ladders.some(l => !l.foot) || needsSettlementTerrainRepair(b.layout)) normalizeBattleCheckpoint(b);
  if (b.phase !== 'combat' || input.paused || !Number.isFinite(dt) || dt <= 0) return b;
  b.accumulator += Math.min(dt, 2);
  b.pendingInput ||= {};
  for (const key of ['attack', 'interact', 'heavy', 'blockPressed']) if (input[key]) b.pendingInput[key] = true;
  if (input.climbLadderId) b.pendingInput.climbLadderId=input.climbLadderId;
  if (input.stairId) b.pendingInput.stairId=input.stairId;
  while (b.accumulator + 1e-9 >= STEP && b.phase === 'combat') {
    b.accumulator -= STEP;
    tick(b, { ...input, attack: input.attack || b.pendingInput.attack, interact: input.interact || b.pendingInput.interact, heavy: input.heavy || b.pendingInput.heavy, blockPressed:!!b.pendingInput.blockPressed, climbLadderId:b.pendingInput.climbLadderId, stairId:b.pendingInput.stairId });
    b.pendingInput = {};
  }
  return b;
}

export function battleResult(b) {
  const armies = b.armies.map(a => {
    const units = b.units.filter(u => u.armyId === a.armyId), survivors = countTypes(units.filter(u => u.status !== 'dead')), losses = {}, escaped = countTypes(units.filter(u => u.status === 'escaped' || u.routing && alive(u))), undeployed = countTypes(units.filter(u => u.status === 'reserve' && !u.wasDeployed));
    for (const [type, n] of Object.entries(a.starting)) { survivors[type] ||= 0; losses[type] = n - survivors[type]; }
    const living = units.filter(u => u.status !== 'dead');
    return { armyId: a.armyId, owner: a.owner, side: a.side, starting: { ...a.starting }, units: survivors, survivors, losses, escaped, undeployed, morale: living.length ? Math.max(.1, living.reduce((sum, u) => sum + u.morale, 0) / living.length / 100) : .1, commanderKnockedOut: units.some(u => u.commander && u.status === 'dead'), destroyedEngines: countTypes(units.filter(u => u.family === 'siege' && u.status === 'dead')), sourceTile: a.sourceTile, arrived: a.arrived, retreat: b.winner !== a.side };
  });
  // A completed legacy save may queue previously deployed survivors to meet
  // the new display cap. Their recorded occupation has already taken place.
  const occupying = b.units.filter(u => (fighting(u) || b.phase === 'complete' && u.status === 'reserve' && u.wasDeployed && !u.routing) && u.side === b.winner && u.family !== 'siege' && (!b.layout.fortified || b.winner === 1 || u.id === b.securedBy || distance(u, b.layout.objective) < (b.layout.castle ? 4 : 14)));
  const sections = [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])];
  const occupier=occupying.find(u=>u.id===b.securedBy)||occupying[0];
  return { battleId: b.encounterId, encounterId: b.encounterId, complete: b.phase === 'complete', winner: b.winner, armies, walls: Math.max(0, b.fortifications.walls), fortIntegrity: Math.max(0, b.fortifications.fortIntegrity), fortifications: clone(b.fortifications), wallBreaches: sections.filter(w => w.hp <= 0).map(w => w.id), wallSections: sections.map(w => ({ id: w.id, hp: w.hp, maxHp: w.maxHp, ...(w.breach ? {breach:{...w.breach}} : {}) })), occupationSide: occupier ? b.winner : null, occupyingArmyId:occupier?.armyId??null, duration: b.time, commanderId: b.commanderId };
}

export function finishAutomatically(b) {
  if (b.phase === 'complete') return battleResult(b);
  beginBattle(b); b.auto = true;
  for (const u of b.units) if (!u.fortPost && !['ladder','breach','crew','retreat'].includes(u.order.kind)) { u.explicitOrder = false; u.order = { kind: 'advance' }; }
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
        const ammunition = u.type === 'ram' ? 0 : u.family === 'ranged' ? u.ammo > 0 ? 1 : .45 : u.family === 'siege' ? u.ammo > 0 ? .8 : .2 : 1;
        const health = .35 + .65 * u.hp / u.maxHp, morale = .3 + .7 * u.morale / 100;
        const attack=UNITS[u.type].attack*(isPlayerHero(b,u)?HERO_WEAPON_DAMAGE:1);
        forces[u.side] += (attack + UNITS[u.type].defense) * ammunition * health * morale;
        living[u.side].push(u);
      }
      if (!living[0].length || !living[1].length) {
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
  if (b.seed !== (encounter.seed ?? encounter.id) || b.playerOwner !== encounter.playerOwner || !Number.isInteger(b.maxActive) || b.maxActive < 4 || b.maxActive > (b.battleRulesVersion>=2?MAX_ACTIVE_TROOPS:240) || !Array.isArray(b.activeCaps)) return false;
  // The original allocator could grant one extra opposing slot when the
  // larger side rounded all the way up to maxActive. Accept that legacy
  // shape so normalization can safely reduce it to the current hard cap.
  const legacyOverflow = (b.battleRulesVersion || 1) < 2 && b.activeCaps[0] === b.maxActive && b.activeCaps[1] === 1 ? 1 : 0;
  if (b.activeCaps.length !== 2 || !b.activeCaps.every(n => Number.isInteger(n) && n >= 0) || b.activeCaps.reduce((n, c) => n + c, 0) > b.maxActive + legacyOverflow) return false;
  if (b.battleRulesVersion >= 2 && (b.maxActive > MAX_ACTIVE_TROOPS || [0, 1].some(side => b.units.filter(u => alive(u) && u.side === side).length > b.activeCaps[side]))) return false;
  if (b.nextReserveWave !== undefined && (!Number.isFinite(b.nextReserveWave) || b.nextReserveWave < 0)) return false;
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
    if (![u.x, u.z, u.y, u.hp, u.maxHp, u.morale, u.stamina, u.ammo, u.facing].every(Number.isFinite) || u.hp < 0 || u.hp > u.maxHp || u.maxHp !== troopMaxHp(b,u) && !hasLegacyHealth(b,u) || u.ammo < 0 || u.ammo > (PROFILE[u.type].ammo || 0) || u.morale < 0 || u.morale > 100 || u.stamina < 0 || u.stamina > 100) return false;
    if (u.status === 'dead' && u.hp !== 0 || u.status !== 'dead' && u.hp <= 0) return false;
    const key = `${u.armyId}:${u.type}`; totals.set(key, (totals.get(key) || 0) + 1);
    if (!u.order || !Array.isArray(u.path) || u.action && ![u.action.elapsed, u.action.duration, u.action.windup].every(Number.isFinite)) return false;
    if(u.family==='mounted'){
      if(u.speed!==undefined&&(!Number.isFinite(u.speed)||u.speed<0||u.speed>20))return false;
      if(u.rideDistance!==undefined&&(!Number.isFinite(u.rideDistance)||u.rideDistance<0))return false;
      if(u.horseLean!==undefined&&(!Number.isFinite(u.horseLean)||Math.abs(u.horseLean)>.22))return false;
      if(u.horseStepAt!==undefined&&(!Number.isFinite(u.horseStepAt)||u.horseStepAt<0||u.horseStepAt>b.time+1e-6))return false;
    }
    if (u.wallId && !b.layout.walls?.some(w => w.id === u.wallId)) return false;
    if (u.wallPosition && (![u.wallPosition.x, u.wallPosition.z, u.wallPosition.facing].every(Number.isFinite) || !b.layout.walls?.some(w => w.id === u.wallPosition.wallId))) return false;
    if (u.action?.kind === 'stairs' && (![u.action.fromX, u.action.fromZ, u.action.fromY, u.action.targetX, u.action.targetZ].every(Number.isFinite) || !b.layout.stairs?.some(s => s.id === u.action.stairId && s.wallId === u.action.wallId))) return false;
  }
  for (const p of encounter.participants) for (const type of Object.keys(UNITS)) if ((p.units?.[type] || 0) !== (totals.get(`${p.armyId}:${type}`) || 0)) return false;
  if (b.commanderId && !b.units.some(u => u.id === b.commanderId && u.owner === encounter.playerOwner && u.commander)) return false;
  if (b.commandGroups !== undefined) {
    if (!Array.isArray(b.commandGroups) || b.commandGroups.length > 12) return false;
    const groupIds = new Set();
    for (const group of b.commandGroups) {
      if (!group || typeof group.id !== 'string' || !group.id.length || group.id.length > 80 || groupIds.has(group.id) || typeof group.name !== 'string' || !group.name.trim() || group.name.length > 40 || !Array.isArray(group.unitIds) || new Set(group.unitIds).size !== group.unitIds.length || !group.unitIds.length) return false;
      groupIds.add(group.id);
      if (group.unitIds.some(id => !b.units.some(u => u.id === id && u.owner === b.playerOwner))) return false;
    }
  }
  if (b.commandSelection !== undefined) {
    const selection = b.commandSelection;
    if (!selection || !['all', 'infantry', 'spearmen', 'archers', 'archer', 'crossbow', 'ranged', 'cavalry', 'siege'].includes(selection.group)) return false;
    if (selection.armyId && !b.armies.some(a => a.armyId === selection.armyId && a.owner === b.playerOwner)) return false;
    if (selection.customGroupId && !b.commandGroups?.some(g => g.id === selection.customGroupId)) return false;
  }
  if (!Array.isArray(b.layout.walls) || !Array.isArray(b.layout.obstacles) || !Array.isArray(b.layout.ladders) || ![0, 1, null].includes(b.winner)) return false;
  if (b.deploymentLayoutVersion !== undefined && b.deploymentLayoutVersion !== 1) return false;
  if (b.layout.deploymentZones !== undefined) {
    const expectedZones = deploymentZonesFor(b);
    if (!Array.isArray(b.layout.deploymentZones) || b.layout.deploymentZones.length !== 2 || b.layout.deploymentZones.some((zone, i) => !zone || Object.keys(expectedZones[i]).some(key => zone[key] !== expectedZones[i][key]))) return false;
  }
  const tile = encounter.tile || {}, fortMax = tile.building === 'fort' ? [0, 45, 100, 180][clamp(tile.levels?.fort || tile.buildings?.fort || tile.level || tile.buildingLevel || 1, 1, 3)] : 0;
  if (!matchesCastleLayout(b.layout,tile) || !validSettlementTerrain(b.layout)) return false;
  const f = b.fortifications;
  if (![f.walls, f.initialWalls, f.fortIntegrity, f.initialFortIntegrity].every(Number.isFinite) || f.initialWalls !== Math.max(0, Number(tile.walls) || 0) || f.initialFortIntegrity !== Math.max(0, Number(tile.fortIntegrity ?? fortMax) || 0)) return false;
  if (f.walls < 0 || f.walls > f.initialWalls || f.fortIntegrity < 0 || f.fortIntegrity > f.initialFortIntegrity) return false;
  for (const w of [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])]) if (![w.x, w.z, w.width, w.depth, w.hp, w.maxHp].every(Number.isFinite) || w.hp < 0 || w.hp > w.maxHp) return false;
  if (b.layout.stairs !== undefined && !Array.isArray(b.layout.stairs)) return false;
  for (const s of b.layout.stairs || []) if (![s.x, s.z, s.topX, s.topZ, s.height].every(Number.isFinite) || !b.layout.walls.some(w => w.id === s.wallId)) return false;
  for (const p of b.projectiles) if (![p.x, p.z, p.y, p.startX, p.startZ, p.startY, p.targetX, p.targetZ, p.targetY, p.elapsed, p.duration, p.damage].every(Number.isFinite) || p.duration <= 0 || p.damage < 0) return false;
  return true;
}
