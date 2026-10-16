export const VERSION = 2;
export const STEP = 1 / 20;
export const TILE = 48;
export const WORLD = Object.freeze({ width: 128, height: 88, seed: 68431 });
// Army keys deliberately match the original Online save schema.
export const UNITS = Object.freeze({
  recruits: {
    name: "Recruit",
    role: "spear",
    tier: 1,
    hp: 42,
    damage: 8,
    armor: 1,
    range: 1.25,
    speed: 2.8,
    cooldown: 1.15,
    cost: 12,
    sheet: 0,
    row: 0,
    upgrades: ["footmen", "archers", "monks"],
  },
  archers: {
    name: "Archer",
    role: "ranged",
    tier: 2,
    hp: 36,
    damage: 10,
    armor: 0,
    range: 8,
    speed: 2.9,
    cooldown: 1.5,
    cost: 24,
    sheet: 0,
    row: 1,
    from: "recruits",
    upgrades: ["veteranArchers", "crossbowmen"],
  },
  footmen: {
    name: "Footman",
    role: "shield",
    tier: 2,
    hp: 70,
    damage: 12,
    armor: 4,
    range: 1.3,
    speed: 2.7,
    cooldown: 1.1,
    cost: 22,
    sheet: 0,
    row: 2,
    from: "recruits",
    upgrades: ["swordsmen", "cavalry"],
  },
  monks: {
    name: "Monk",
    role: "healer",
    tier: 2,
    hp: 46,
    damage: 7,
    armor: 1,
    range: 5,
    speed: 2.8,
    cooldown: 1.6,
    cost: 26,
    sheet: 0,
    row: 3,
    from: "recruits",
    upgrades: ["priests", "mages"],
  },
  veteranArchers: {
    name: "Veteran archer",
    role: "ranged",
    tier: 3,
    hp: 52,
    damage: 17,
    armor: 2,
    range: 10,
    speed: 3,
    cooldown: 1.3,
    cost: 50,
    sheet: 0,
    row: 4,
    from: "archers",
    upgrades: [],
  },
  crossbowmen: {
    name: "Crossbowman",
    role: "crossbow",
    tier: 3,
    hp: 58,
    damage: 24,
    armor: 3,
    range: 8,
    speed: 2.6,
    cooldown: 2,
    cost: 48,
    sheet: 0,
    row: 5,
    from: "archers",
    upgrades: [],
  },
  swordsmen: {
    name: "Swordsman",
    role: "shield",
    tier: 3,
    hp: 104,
    damage: 19,
    armor: 7,
    range: 1.4,
    speed: 2.9,
    cooldown: 1,
    cost: 55,
    sheet: 0,
    row: 6,
    from: "footmen",
    upgrades: ["romanSergeants", "romanChampions"],
  },
  cavalry: {
    name: "Cavalry",
    role: "cavalry",
    tier: 3,
    hp: 110,
    damage: 20,
    armor: 4,
    range: 1.6,
    speed: 4.9,
    cooldown: 1.3,
    cost: 68,
    sheet: 1,
    row: 0,
    from: "footmen",
    upgrades: ["dragonKnights"],
  },
  priests: {
    name: "Priest",
    role: "healer",
    tier: 3,
    hp: 72,
    damage: 15,
    armor: 2,
    range: 7,
    speed: 2.8,
    cooldown: 1.4,
    cost: 50,
    sheet: 1,
    row: 1,
    from: "monks",
    upgrades: ["venetianSaints"],
  },
  mages: {
    name: "Mage",
    role: "mage",
    tier: 3,
    hp: 50,
    damage: 22,
    armor: 0,
    range: 8,
    speed: 2.8,
    cooldown: 2.3,
    cost: 72,
    sheet: 1,
    row: 2,
    from: "monks",
    upgrades: [],
  },
  romanSergeants: {
    name: "Roman sergeant",
    role: "spear",
    tier: 4,
    hp: 150,
    damage: 25,
    armor: 9,
    range: 1.6,
    speed: 2.9,
    cooldown: 1,
    cost: 160,
    sheet: 1,
    row: 3,
    from: "swordsmen",
    upgrades: [],
  },
  romanChampions: {
    name: "Roman champion",
    role: "shield",
    tier: 4,
    hp: 185,
    damage: 32,
    armor: 12,
    range: 1.4,
    speed: 2.6,
    cooldown: 1.05,
    cost: 190,
    sheet: 1,
    row: 4,
    from: "swordsmen",
    upgrades: [],
  },
  dragonKnights: {
    name: "Dragon knight",
    role: "cavalry",
    tier: 4,
    hp: 190,
    damage: 34,
    armor: 10,
    range: 1.8,
    speed: 5.2,
    cooldown: 1.15,
    cost: 230,
    sheet: 1,
    row: 5,
    from: "cavalry",
    upgrades: [],
  },
  venetianSaints: {
    name: "Venetian saint",
    role: "healer",
    tier: 4,
    hp: 130,
    damage: 26,
    armor: 7,
    range: 8,
    speed: 3,
    cooldown: 1.1,
    cost: 200,
    sheet: 1,
    row: 6,
    from: "priests",
    upgrades: [],
  },
});
export const UNIT_KEYS = Object.keys(UNITS);
export const ORDERS = [
  "move",
  "attack",
  "charge",
  "hold",
  "defend",
  "follow",
  "retreat",
  "patrol",
  "auto",
];
export const FORMATIONS = ["line", "shieldwall", "square", "wedge", "loose"];
export const FACTIONS = [
  {
    id: "crown",
    name: "The Crownlands",
    color: "#5ba9d4",
    trait: "Disciplined shield walls",
    units: ["footmen", "archers", "romanSergeants"],
  },
  {
    id: "ember",
    name: "Ember March",
    color: "#db7956",
    trait: "Cavalry flanks and fire magic",
    units: ["cavalry", "mages", "swordsmen"],
  },
  {
    id: "vale",
    name: "The Green Vale",
    color: "#85b780",
    trait: "Longbows and restorative magic",
    units: ["veteranArchers", "priests", "recruits"],
  },
];
export const ITEMS = {
  wayfarer: {
    name: "Wayfarer standard",
    description: "Travel 15% faster; your first contract reward.",
    cost: 140,
  },
  bulwark: {
    name: "Banner of the Bulwark",
    description: "Shield formations reduce incoming damage by another 8%.",
    cost: 180,
  },
  sun: {
    name: "Sunward pennant",
    description: "Healers restore 20% more health.",
    cost: 180,
  },
  quartermaster: {
    name: "Quartermaster seal",
    description: "Supplies last 25% longer.",
    cost: 160,
  },
};
export const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
export const clone = (v) => JSON.parse(JSON.stringify(v));
export const armySize = (army) =>
  UNIT_KEYS.reduce((n, k) => n + (army?.[k] || 0), 0);
export const armyPower = (army) =>
  UNIT_KEYS.reduce((n, k) => n + (army?.[k] || 0) * UNITS[k].tier ** 1.55, 0);
export function rng(seed = 1) {
  let value = seed >>> 0;
  return () => {
    value = (Math.imul(1664525, value) + 1013904223) >>> 0;
    return value / 4294967296;
  };
}
export function hash(x, y, seed = WORLD.seed) {
  let h = Math.imul(x + seed, 374761393) ^ Math.imul(y + seed, 668265263);
  h = Math.imul(h ^ (h >>> 13), 1274126177);
  return (h ^ (h >>> 16)) >>> 0;
}
export function emptyArmy() {
  return Object.fromEntries(UNIT_KEYS.map((k) => [k, 0]));
}
export function safeArmy(raw = {}) {
  return Object.fromEntries(
    UNIT_KEYS.map((k) => [k, clamp(Math.floor(Number(raw[k]) || 0), 0, 10000)]),
  );
}
export function newProfile(username = "Wanderer") {
  return {
    schemaVersion: VERSION,
    username,
    gold: 260,
    supplies: 100,
    renown: 0,
    level: 1,
    xp: 0,
    army: { ...emptyArmy(), recruits: 12, footmen: 6, archers: 6, monks: 2 },
    reserve: emptyArmy(),
    x: 15.5,
    y: 44.5,
    morale: 100,
    inventory: { banners: [], equipped: [] },
    achievements: [],
    stats: { wins: 0, battles: 0, contracts: 0, castles: 0 },
    relations: { crown: 0, ember: 0, vale: 0 },
    skills: { leadership: 0, logistics: 0, tactics: 0 },
    skillPoints: 0,
    contracts: [],
    claimedContracts: [],
    explored: [],
    activeBattle: null,
    revision: 0,
    lastIncome: 0,
  };
}
export function armyCap(p) {
  return Math.min(
    600,
    60 + (p.level - 1) * 12 + (p.skills?.leadership || 0) * 15,
  );
}
export function addXP(p, amount) {
  p.xp += Math.max(0, amount);
  while (p.xp >= 100 * p.level && p.level < 100) {
    p.xp -= 100 * p.level;
    p.level++;
    p.skillPoints++;
  }
  if (p.level === 100) p.xp = Math.min(p.xp, 9999);
}
export function migrateLegacy(raw = {}, username = raw.user || "Wanderer") {
  const p = newProfile(username);
  p.gold = Math.max(0, Number(raw.gold) || 0);
  p.level = clamp(Math.floor(raw.level) || 1, 1, 100);
  p.xp = Math.max(0, Number(raw.xp) || 0);
  p.army = safeArmy(raw.army);
  p.inventory = clone(raw.inventory || p.inventory);
  p.inventory.banners = Array.isArray(p.inventory.banners)
    ? p.inventory.banners
    : [];
  p.inventory.equipped = (
    Array.isArray(p.inventory.equipped) ? p.inventory.equipped : []
  )
    .filter((key) => typeof key === "string" && ITEMS[key])
    .slice(0, 3);
  // Keep unknown legacy banner payloads for review, even though only known bonuses apply.
  p.legacyInventory = clone(raw.inventory || {});
  p.legacyAllegiance = raw.allegianceId || null;
  p.x = clamp(((Number(raw.x) || 15) * WORLD.width) / 180, 1, WORLD.width - 2);
  p.y = clamp(
    ((Number(raw.y) || 44) * WORLD.height) / 120,
    1,
    WORLD.height - 2,
  );
  // Excess forces are retained in reserve; imported progression is never truncated to the new field cap.
  let room = armyCap(p);
  for (const k of [...UNIT_KEYS].reverse()) {
    const field = Math.min(room, p.army[k]);
    p.reserve[k] = p.army[k] - field;
    p.army[k] = field;
    room -= field;
  }
  p.migrated = true;
  return p;
}
