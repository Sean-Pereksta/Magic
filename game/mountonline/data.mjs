export const VERSION = 2;
export const STEP = 1 / 20;
export const TILE = 48;
export const WORLD = Object.freeze({ width: 320, height: 224, seed: 68431, worldVersion: 2 });
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
  towerWardens: { name: "Tower Warden", faction: "crown", role: "shield", tier: 3, hp: 136, damage: 15, armor: 10, range: 1.4, speed: 2.35, cooldown: 1.2, cost: 96, sheet: 3, row: 0, shield: 1.25, stamina: 120, upgrades: [] },
  royalCaptains: { name: "Royal Captain", faction: "crown", role: "shield", tier: 4, hp: 142, damage: 24, armor: 8, range: 1.4, speed: 2.8, cooldown: 1.1, cost: 180, sheet: 3, row: 1, shield: 1, officer: true, stamina: 110, upgrades: [] },
  ashBombardiers: { name: "Ash Bombardier", faction: "ember", role: "mage", tier: 3, hp: 48, damage: 26, armor: 1, range: 7.5, speed: 2.5, cooldown: 3.1, cost: 100, sheet: 3, row: 2, splashRadius: 2.1, splashFalloff: .7, friendlyFire: .2, ammo: 16, upgrades: [] },
  cinderLancers: { name: "Cinder Lancer", faction: "ember", role: "cavalry", tier: 4, hp: 144, damage: 25, armor: 7, range: 1.9, speed: 5.1, cooldown: 1.5, cost: 165, sheet: 3, row: 3, charge: 1.35, stamina: 110, upgrades: [] },
  thornbowRangers: { name: "Thornbow Ranger", faction: "vale", role: "ranged", tier: 3, hp: 56, damage: 18, armor: 2, range: 10.5, speed: 3.2, cooldown: 1.35, cost: 82, sheet: 3, row: 4, terrainAffinity: "forest", ammo: 36, upgrades: [] },
  groveSentinels: { name: "Grove Sentinel", faction: "vale", role: "spear", tier: 3, hp: 106, damage: 20, armor: 6, range: 1.9, speed: 2.9, cooldown: 1.2, cost: 86, sheet: 3, row: 5, brace: 1.2, terrainAffinity: "forest", upgrades: [] },
  falconHorseArchers: { name: "Falcon Horse Archer", faction: "steppe", role: "horseArcher", tier: 3, hp: 76, damage: 13, armor: 2, range: 8.5, speed: 5.3, cooldown: 1.65, cost: 112, sheet: 3, row: 6, ammo: 30, stamina: 100, terrainAffinity: "steppe", upgrades: [] },
  khaganLancers: { name: "Khagan Lancer", faction: "steppe", role: "cavalry", tier: 4, hp: 132, damage: 24, armor: 6, range: 1.9, speed: 5.4, cooldown: 1.45, cost: 160, sheet: 3, row: 7, charge: 1.25, stamina: 125, upgrades: [] },
  graniteShieldbearers: { name: "Granite Shieldbearer", faction: "stone", role: "shield", tier: 3, hp: 154, damage: 18, armor: 12, range: 1.3, speed: 2.15, cooldown: 1.25, cost: 112, sheet: 3, row: 8, shield: 1.4, stamina: 135, terrainAffinity: "mountain", upgrades: [] },
  thunderSlingers: { name: "Thunder Slinger", faction: "stone", role: "mage", tier: 3, hp: 65, damage: 23, armor: 4, range: 8, speed: 2.5, cooldown: 2.6, cost: 90, sheet: 3, row: 9, splashRadius: 1.35, splashFalloff: .8, friendlyFire: 0, ammo: 24, upgrades: [] },
  paviseCrossbowmen: { name: "Pavise Crossbowman", faction: "suncoast", role: "crossbow", tier: 3, hp: 76, damage: 27, armor: 5, range: 8.5, speed: 2.45, cooldown: 2.35, cost: 106, sheet: 3, row: 10, shield: .65, ammo: 28, upgrades: [] },
  condottieriPikes: { name: "Condottieri Pike", faction: "suncoast", role: "spear", tier: 3, hp: 108, damage: 21, armor: 7, range: 2.25, speed: 2.5, cooldown: 1.3, cost: 102, sheet: 3, row: 11, brace: 1.5, upgrades: [] },
  hearthHuscarls: { name: "Hearth Huscarl", faction: "frost", role: "shield", tier: 3, hp: 122, damage: 24, armor: 7, range: 1.5, speed: 2.8, cooldown: 1.1, cost: 100, sheet: 3, row: 12, shield: 1.1, terrainAffinity: "snow", upgrades: [] },
  ravagerAxemen: { name: "Ravager Axeman", faction: "frost", role: "shield", tier: 3, hp: 108, damage: 31, armor: 4, range: 1.5, speed: 3.1, cooldown: 1.35, cost: 96, sheet: 3, row: 13, shield: .35, armorPierce: .25, stamina: 115, upgrades: [] },
  duneRiders: { name: "Dune Rider", faction: "desert", role: "cavalry", tier: 3, hp: 96, damage: 18, armor: 3, range: 1.7, speed: 5.6, cooldown: 1.25, cost: 104, sheet: 3, row: 14, charge: 1.1, terrainAffinity: "desert", stamina: 125, upgrades: [] },
  sandstormSkirmishers: { name: "Sandstorm Skirmisher", faction: "desert", role: "ranged", tier: 3, hp: 60, damage: 17, armor: 2, range: 6.5, speed: 3.5, cooldown: 1.25, cost: 78, sheet: 3, row: 15, ammo: 26, terrainAffinity: "desert", upgrades: [] },
  ironSappers: { name: "Iron Sapper", faction: "iron", role: "mage", tier: 3, hp: 84, damage: 24, armor: 7, range: 6.5, speed: 2.35, cooldown: 3, cost: 112, sheet: 3, row: 16, splashRadius: 1.8, splashFalloff: .75, friendlyFire: .15, siegeBonus: 2, ammo: 18, upgrades: [] },
  chainFlailGuards: { name: "Chain Flail Guard", faction: "iron", role: "shield", tier: 4, hp: 151, damage: 26, armor: 10, range: 1.7, speed: 2.45, cooldown: 1.7, cost: 174, sheet: 3, row: 17, shield: .6, splashRadius: 1.5, splashFalloff: .65, friendlyFire: 0, upgrades: [] },
  reedAmbushers: { name: "Reed Ambusher", faction: "marsh", role: "ranged", tier: 3, hp: 62, damage: 18, armor: 2, range: 7, speed: 3.3, cooldown: 1.35, cost: 80, sheet: 3, row: 18, terrainAffinity: "marsh", ammo: 30, upgrades: [] },
  mireHurlers: { name: "Mire Hurler", faction: "marsh", role: "mage", tier: 3, hp: 68, damage: 20, armor: 3, range: 7, speed: 2.8, cooldown: 2.65, cost: 92, sheet: 3, row: 19, terrainAffinity: "marsh", splashRadius: 1.65, splashFalloff: .8, friendlyFire: 0, ammo: 22, upgrades: [] },
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
  "attackNearest",
  "skirmish",
  "flank",
  "fallback",
  "focus",
  "regroup",
  "brace",
];
export const FORMATIONS = ["line", "shieldwall", "square", "wedge", "loose"];
export const FACTIONS = [
  {
    id: "crown",
    name: "The Crownlands",
    color: "#5ba9d4",
    trait: "Disciplined shield walls",
    units: ["footmen", "archers", "romanSergeants"],
    doctrine: "defensive", personality: "honor", leader: "Queen Aldara", economy: ["food", "equipment"], priorities: ["borders", "stability"], signatureUnits: ["towerWardens", "royalCaptains"],
  },
  {
    id: "ember",
    name: "Ember March",
    color: "#db7956",
    trait: "Cavalry flanks and fire magic",
    units: ["cavalry", "mages", "swordsmen"],
    doctrine: "aggressive", personality: "expansionist", leader: "Marshal Cadran", economy: ["metal", "horses"], priorities: ["conquest", "raids"], signatureUnits: ["ashBombardiers", "cinderLancers"],
  },
  {
    id: "vale",
    name: "The Green Vale",
    color: "#85b780",
    trait: "Longbows and restorative magic",
    units: ["veteranArchers", "priests", "recruits"],
    doctrine: "forest", personality: "protective", leader: "Warden Elowen", economy: ["timber", "food"], priorities: ["forests", "defense"], signatureUnits: ["thornbowRangers", "groveSentinels"],
  },
  { id: "steppe", name: "Steppe Khanate", color: "#c5aa5e", trait: "Mounted archery and mobile lancers", doctrine: "steppe", personality: "strength", leader: "Khagan Temur", economy: ["horses", "food"], priorities: ["tribute", "passage"], units: ["cavalry", "archers", "recruits"], signatureUnits: ["falconHorseArchers", "khaganLancers"] },
  { id: "stone", name: "Stonehold Clans", color: "#9a9eb5", trait: "Unyielding mountain shield lines", doctrine: "mountain", personality: "stalwart", leader: "High Thane Brokk", economy: ["metal", "equipment"], priorities: ["passes", "defense"], units: ["footmen", "romanSergeants", "crossbowmen"], signatureUnits: ["graniteShieldbearers", "thunderSlingers"] },
  { id: "suncoast", name: "Suncoast League", color: "#e5c379", trait: "Pavises, pikes and wealthy mercenaries", doctrine: "mercenary", personality: "commercial", leader: "Doge Alessia", economy: ["equipment", "trade"], priorities: ["trade", "caravans"], units: ["crossbowmen", "recruits", "romanSergeants"], signatureUnits: ["paviseCrossbowmen", "condottieriPikes"] },
  { id: "frost", name: "Frostbound Jarldoms", color: "#b1d4df", trait: "Fearsome raiders and shield formations", doctrine: "raiding", personality: "feuding", leader: "High Jarl Sigrid", economy: ["timber", "equipment"], priorities: ["honor", "raids"], units: ["swordsmen", "footmen", "archers"], signatureUnits: ["hearthHuscarls", "ravagerAxemen"] },
  { id: "desert", name: "Desert Sultanate", color: "#d5a779", trait: "Swift cavalry and agile skirmishers", doctrine: "desert", personality: "pragmatic", leader: "Sultana Nadiya", economy: ["horses", "trade"], priorities: ["caravans", "leverage"], units: ["cavalry", "archers", "recruits"], signatureUnits: ["duneRiders", "sandstormSkirmishers"] },
  { id: "iron", name: "Iron Covenant", color: "#9e788c", trait: "Armored siege armies and engineers", doctrine: "siege", personality: "conqueror", leader: "First Prelate Voss", economy: ["metal", "equipment"], priorities: ["fortifications", "vassals"], units: ["swordsmen", "crossbowmen", "mages"], signatureUnits: ["ironSappers", "chainFlailGuards"] },
  { id: "marsh", name: "Marsh Freeholds", color: "#8da16d", trait: "Concealed archers and disruptive missiles", doctrine: "ambush", personality: "independent", leader: "Speaker Fenna", economy: ["food", "timber"], priorities: ["independence", "coalitions"], units: ["archers", "recruits", "footmen"], signatureUnits: ["reedAmbushers", "mireHurlers"] },
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
    worldVersion: WORLD.worldVersion,
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
    relations: Object.fromEntries(FACTIONS.map((f) => [f.id, 0])),
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
  // Legacy armies entered the original western realm; expansion does not move them east.
  p.x = clamp(((Number(raw.x) || 15) * 128) / 180, 1, WORLD.width - 2);
  p.y = clamp(
    ((Number(raw.y) || 44) * 88) / 120,
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
