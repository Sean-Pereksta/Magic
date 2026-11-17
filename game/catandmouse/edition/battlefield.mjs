export const STEP_MS = 50;

const millSize = 48;
const millTiles = Array.from({ length: millSize }, (_, y) =>
  Array.from({ length: millSize }, (_, x) => {
    if (y === 0 || x === 0 || y === millSize - 1 || x === millSize - 1) return '#';
    if ((y >= 22 && y <= 25) || (x >= 22 && x <= 25)) return ':';
    return '.';
  }),
);

function fillMill(x, y, width, height, tile) {
  for (let row = y; row < y + height; row++) {
    for (let column = x; column < x + width; column++) {
      millTiles[row][column] = tile;
    }
  }
}

// Forebay, mill race, grain racks and flour bins leave four clear approaches.
fillMill(3, 6, 13, 9, '~');
fillMill(3, 16, 7, 2, '~');
fillMill(6, 14, 4, 3, '~');
fillMill(17, 4, 2, 11, '#');
fillMill(5, 19, 11, 1, '#');
fillMill(28, 6, 7, 3, '#');
fillMill(37, 10, 4, 4, '#');
fillMill(29, 13, 2, 6, '#');
fillMill(35, 17, 8, 1, '#');
fillMill(6, 28, 3, 11, '#');
fillMill(11, 34, 6, 4, '#');
fillMill(13, 42, 6, 2, '#');
fillMill(30, 34, 3, 9, '#');
fillMill(34, 39, 10, 2, '#');
fillMill(39, 29, 3, 5, '#');
fillMill(17, 27, 1, 4, '#');
fillMill(31, 26, 1, 5, '#');

const flourMill = Object.freeze({
  id: 'flour-mill',
  name: 'The Flour Mill',
  size: millSize,
  description: 'Defend the millstone, flour sacks and hoist at the crossroads of four rat approaches.',
  tiles: Object.freeze(millTiles.map(row => row.join(''))),
  spawns: Object.freeze([
    { id: 'west', name: 'Mill Race', x: 1, y: 23 },
    { id: 'north', name: 'Grain Chute', x: 23, y: 1 },
    { id: 'east', name: 'Loading Door', x: 46, y: 24 },
    { id: 'south', name: 'Cart Ramp', x: 24, y: 46 },
  ].map(Object.freeze)),
  playerSpawns: Object.freeze([
    { x: 23, y: 26 }, { x: 25, y: 25 }, { x: 22, y: 24 }, { x: 25, y: 22 },
  ].map(Object.freeze)),
  objectives: Object.freeze([
    { id: 'millstone', name: 'Millstone', x: 23, y: 23, maxHp: 105 },
    { id: 'sacks', name: 'Flour Sacks', x: 20, y: 27, maxHp: 85 },
    { id: 'hoist', name: 'Sack Hoist', x: 27, y: 26, maxHp: 85 },
  ].map(Object.freeze)),
  cheese: Object.freeze([
    { x: 21, y: 25 }, { x: 24, y: 21 }, { x: 25, y: 28 }, { x: 28, y: 24 },
    { x: 19, y: 23 }, { x: 23, y: 32 }, { x: 29, y: 28 }, { x: 23, y: 17 },
    { x: 33, y: 24 }, { x: 14, y: 23 }, { x: 24, y: 37 }, { x: 23, y: 10 },
  ].map(Object.freeze)),
});

export const MAPS = Object.freeze({ [flourMill.id]: flourMill });

export const DIFFICULTIES = Object.freeze({
  easy: Object.freeze({ name: 'Easy', health: 0.82, damage: 0.72, waveBonus: 0, openingCheese: 30 }),
  normal: Object.freeze({ name: 'Normal', health: 1, damage: 1, waveBonus: 0, openingCheese: 26 }),
  hard: Object.freeze({ name: 'Hard', health: 1.18, damage: 1.16, waveBonus: 3, openingCheese: 25, catSweep: true }),
  nightmare: Object.freeze({ name: 'Nightmare', health: 1.36, damage: 1.30, waveBonus: 5, openingCheese: 24, catSweep: true, catCharge: true }),
});

const towerDefinitions = {
  wall: { name: 'Wall', role: 'Defense', cost: 1, hp: 25, description: 'Blocks rat approaches and buys nearby towers time to fire.' },
  turret: {
    name: 'Turret', role: 'Attack', cost: 3, hp: 22, range: 6.5, cooldown: 690, windup: 170, kind: 'bullet', damage: 8,
    description: 'Fires quick rounds at the closest rat in sight.',
    upgrades: {
      marksman: { name: 'Long Barrel', cost: 9, range: 9.5, cooldown: 730, damage: 13, description: 'More range and damage per bullet.' },
      burst: { name: 'Burst Chamber', cost: 8, range: 5.1, cooldown: 670, burst: 3, damage: 5, description: 'Fires three rounds at closer targets.' },
    },
  },
  generator: { name: 'Generator', role: 'Economy', cost: 4, hp: 19, cooldown: 4800, description: 'Produces cheese for its owner every few seconds.' },
  rocket: {
    name: 'Rocket Tower', role: 'Attack', cost: 12, hp: 25, range: 8.4, cooldown: 1920, windup: 430, kind: 'rocket', damage: 24, splash: 1.5,
    description: 'Targets bulky enemies with explosive rockets.',
    upgrades: {
      payload: { name: 'Heavy Payload', cost: 12, range: 9.2, cooldown: 1940, damage: 49, splash: 0, description: 'A concentrated hit against one enemy.' },
      cluster: { name: 'Cluster Warhead', cost: 12, cooldown: 1800, damage: 23, splash: 2.7, description: 'Wider blast radius against packed waves.' },
    },
  },
  repair: { name: 'Repair Station', role: 'Support', cost: 7, hp: 24, range: 4, cooldown: 1200, description: 'Repairs nearby structures and shared objectives.' },
  acorn: { name: 'Acorn Tower', role: 'Attack', cost: 8, hp: 21, range: 7.6, cooldown: 1350, windup: 280, kind: 'acorn', damage: 19, description: 'Lobs heavy acorns over terrain and defensive walls.' },
  web: { name: 'Web Launcher', role: 'Control', cost: 6, hp: 19, range: 6.8, cooldown: 2050, windup: 245, kind: 'web', damage: 1, description: 'Traps a target before briefly slowing it.' },
  rabbit: { name: 'Rabbit Den', role: 'Producer', cost: 8, hp: 27, cooldown: 7500, description: 'Produces one rabbit that helps defend the mill.' },
  flea: { name: 'Flea Nest', role: 'Producer', cost: 5, hp: 19, cooldown: 5700, description: 'Produces a flea that slows rats in close quarters.' },
  barricade: { name: 'Super Wall', role: 'Defense', cost: 6, hp: 88, description: 'Absorbs sustained attacks at crowded entrances.' },
  tesla: { name: 'Tesla Tower', role: 'Attack', cost: 11, hp: 22, range: 4.4, cooldown: 1060, windup: 280, damage: 14, description: 'Chains an arc between up to three nearby enemies.' },
  laser: { name: 'Ultra Laser', role: 'Attack', cost: 14, hp: 24, range: 8.7, cooldown: 1660, windup: 450, damage: 34, description: 'Charges and delivers an instant focused beam.' },
  shield: { name: 'Shield Tower', role: 'Support', cost: 8, hp: 26, range: 3.5, cooldown: 300, description: 'Recharges shields on nearby structures.' },
  slime: { name: 'Slime Tower', role: 'Control', cost: 7, hp: 22, range: 3.6, cooldown: 250, description: 'Slows rats in a short-range aura.' },
  drum: { name: 'Rally Drum', role: 'Support', cost: 8, hp: 24, range: 3.5, cooldown: 2000, description: 'Speeds up nearby towers and allied attacks.' },
  farm: { name: 'Cheese Farm', role: 'Economy', cost: 9, hp: 23, cooldown: 7200, description: 'Produces cheese that any teammate can gather.' },
};

export const TOWERS = Object.freeze(Object.fromEntries(Object.entries(towerDefinitions).map(([id, def]) => {
  const upgrades = def.upgrades && Object.freeze(Object.fromEntries(
    Object.entries(def.upgrades).map(([key, value]) => [key, Object.freeze(value)]),
  ));
  return [id, Object.freeze({ ...def, id, upgrades })];
})));

export const STARTER_LOADOUT = Object.freeze(['wall', 'turret', 'generator', 'rocket', 'repair', 'acorn', 'web', 'rabbit']);

export function isCompleteLoadout(loadout) {
  return Array.isArray(loadout) && loadout.length === STARTER_LOADOUT.length
    && new Set(loadout).size === STARTER_LOADOUT.length
    && Array.from(loadout).every(id => typeof id === 'string' && Object.hasOwn(TOWERS, id));
}

export function normalizeLoadout(loadout) {
  if (isCompleteLoadout(loadout)) return loadout.slice();
  const used = new Set();
  const slots = STARTER_LOADOUT.map((_, index) => {
    const id = Array.isArray(loadout) ? loadout[index] : null;
    if (typeof id !== 'string' || !Object.hasOwn(TOWERS, id) || used.has(id)) return null;
    used.add(id);
    return id;
  });
  const available = [...STARTER_LOADOUT, ...Object.keys(TOWERS)].filter((id, index, all) =>
    !used.has(id) && all.indexOf(id) === index,
  );
  return slots.map(id => id ?? available.shift());
}

export const ENEMIES = Object.freeze({
  rat: Object.freeze({ hp: 22, speed: 2.14, damage: 4, reach: 0.83, windup: 220, cooldown: 1060, radius: 0.4, cheese: 2 }),
  runner: Object.freeze({ hp: 15, speed: 3.2, damage: 3, reach: 0.79, windup: 130, cooldown: 760, radius: 0.36, cheese: 2 }),
  stink: Object.freeze({ hp: 34, speed: 1.78, damage: 5, reach: 0.88, windup: 430, cooldown: 1130, radius: 0.43, cheese: 3 }),
  ox: Object.freeze({ hp: 75, speed: 1.52, damage: 12, reach: 1.04, windup: 530, cooldown: 1490, radius: 0.55, cheese: 5 }),
  vulture: Object.freeze({ hp: 29, speed: 2.63, damage: 6, reach: 0.88, windup: 310, cooldown: 1030, radius: 0.41, cheese: 3 }),
  king: Object.freeze({ hp: 92, speed: 1.57, damage: 10, reach: 1, windup: 430, cooldown: 1250, radius: 0.53, cheese: 6 }),
  sapper: Object.freeze({ hp: 59, speed: 1.77, damage: 5, reach: 0.83, windup: 360, cooldown: 1200, radius: 0.45, cheese: 5 }),
  mender: Object.freeze({ hp: 42, speed: 1.86, damage: 4, reach: 0.83, windup: 330, cooldown: 1225, radius: 0.43, cheese: 4 }),
  cat: Object.freeze({ hp: 288, speed: 2.05, damage: 15, reach: 1.15, windup: 560, cooldown: 1410, radius: 0.67, cheese: 10 }),
});

export const SHOTS = Object.freeze({
  bullet: Object.freeze({ speed: 10.5, lifetime: 1100, radius: 0.09, terrain: true }),
  rabbit: Object.freeze({ speed: 8.4, lifetime: 950, radius: 0.095, terrain: true }),
  rocket: Object.freeze({ speed: 6.7, lifetime: 1650, radius: 0.19, terrain: true }),
  acorn: Object.freeze({ speed: 6.2, lifetime: 1530, radius: 0.2, terrain: false }),
  web: Object.freeze({ speed: 7.8, lifetime: 1100, radius: 0.16, terrain: true }),
});
