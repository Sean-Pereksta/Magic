/** Spell Tower's deterministic, JSON-only rules. No DOM, clock, or network state. */
export const COLS = 4;
export const ROWS = 10;
export const TOWER_ROW = ROWS - 1;
export const MAX_PLAYERS = 8;
export const MAX_HAND = 14;
export const LAST_WAVE = 10;
export const BASE_ELEMENTS = ['fire', 'water', 'nature', 'lightning', 'ice', 'earth', 'wind'];
export const DIFFICULTIES = {
  easy: { label: 'Apprentice', hp: 80, mana: 7, scale: 0.82 },
  normal: { label: 'Mage', hp: 65, mana: 6, scale: 1 },
  hard: { label: 'Archmage', hp: 50, mana: 5, scale: 1.22 }
};

const spell = (key, name, element, pattern, damage, description, parts = [element], tier = 0) =>
  ({ key, name, element, pattern, damage, description, parts, tier, cost: tier ? 2 : 1, charges: 3 });
export const SPELLS = Object.fromEntries([
  spell('fire', 'Firebolt', 'fire', 'cell', 5, 'Strike one tile. Ignite vines and poison; boil puddles or shatter frost.'),
  spell('water', 'Rainfall', 'water', 'cross', 1, 'Leave linked puddles for 3 rounds. Soak foes, put out flames and cleanse poison.'),
  spell('nature', 'Vines', 'nature', 'cross', 1, 'Root a cross of tiles for 2 rounds. Water grows the vines; fire turns them into a fierce blaze.'),
  spell('lightning', 'Spark', 'lightning', 'chain', 4, 'Chain through touching foes or linked puddles. Wet foes take +3 damage and lose a round to stun.'),
  spell('ice', 'Frost', 'ice', 'cross', 2, 'Chill a cross for 2 rounds. Wet foes freeze longer; fire and stone shatter frozen foes.'),
  spell('earth', 'Stone', 'earth', 'cell', 5, 'Raise a temporary barricade. Shatter frozen foes; puddles turn to slowing mud.'),
  spell('wind', 'Gust', 'wind', 'column', 2, 'Push a lane back one tile. Spread nearby flames, puddles or spores; scatter ice splinters.'),
  spell('steam', 'Steam Burst', 'water', 'area', 3, 'Scald a 3×3 area and leave blinding mist. Lightning electrifies the mist.', ['fire', 'water'], 1),
  spell('wildfire', 'Bramble Blaze', 'fire', 'area', 3, 'Grow a thicket, then ignite it: extra impact damage and fierce lingering fire.', ['nature', 'fire'], 1),
  spell('plasma', 'Plasma Arc', 'lightning', 'chain', 5, 'Chain lightning through a cluster, then leave scorching plasma at each struck tile.', ['lightning', 'fire'], 1),
  spell('thermal', 'Thermal Shock', 'fire', 'cross', 3, 'Flash-freeze a cross, then shatter it with heat for a powerful burst and steam.', ['ice', 'fire'], 1),
  spell('eruption', 'Eruption', 'fire', 'area', 4, 'Raise molten stones across an area, then erupt into lasting, powerful fire.', ['earth', 'fire'], 1),
  spell('firestorm', 'Firestorm', 'fire', 'column', 4, 'Burn a lane, drive enemies back, and fan the flames into adjacent tiles.', ['fire', 'wind'], 1),
  spell('overgrowth', 'Living Rain', 'nature', 'area', 2, 'Soak an area and grow long-lasting vines through it. Restore 3 tower health.', ['water', 'nature'], 1),
  spell('tempest', 'Thunderpool', 'lightning', 'area', 4, 'Flood an area, then conduct lightning through connected puddles: bonus damage and stun.', ['water', 'lightning'], 1),
  spell('blizzard', 'Flash Freeze', 'ice', 'area', 3, 'Drench and freeze a whole area. Frozen enemies miss 2 rounds.', ['water', 'ice'], 1),
  spell('mire', 'Quagmire', 'earth', 'area', 3, 'Turn a soaked area to deep mud that slows enemies for 3 rounds.', ['water', 'earth'], 1),
  spell('tidal', 'Tidal Surge', 'water', 'column', 4, 'Wash a lane, spread puddles and push enemies back 2 tiles.', ['water', 'wind'], 1),
  spell('stormroot', 'Stormroot', 'lightning', 'cross', 4, 'Make living conductors: root enemies, then shock and stun the thicket.', ['nature', 'lightning'], 1),
  spell('frostbloom', 'Frostbloom', 'ice', 'area', 3, 'Freeze a living thicket. Enemies remain rooted when the ice melts.', ['nature', 'ice'], 1),
  spell('briarwall', 'Briar Rampart', 'earth', 'row', 3, 'Raise a thorny line of barricades; strengthen existing vines and block a wave.', ['nature', 'earth'], 1),
  spell('spores', 'Spore Gust', 'nature', 'area', 2, 'Seed poisonous spores across an area, then scatter them with a gust. Fire detonates spores.', ['nature', 'wind'], 1),
  spell('hailstorm', 'Hailstorm', 'lightning', 'area', 4, 'Shatter frost into charged shards; chill and stun an area.', ['ice', 'lightning'], 1),
  spell('lodestone', 'Lodestone', 'earth', 'area', 5, 'Magnetize foes: expose them to +25% damage for 2 rounds, strike them, then raise cover.', ['lightning', 'earth'], 1),
  spell('thundercloud', 'Thunder Gale', 'lightning', 'column', 5, 'Shock an entire lane and push it back. Wet foes conduct the storm into neighboring puddles.', ['lightning', 'wind'], 1),
  spell('glacier', 'Glacier Break', 'earth', 'area', 4, 'Freeze an area and smash it with stone for shatter damage; leave barricades.', ['ice', 'earth'], 1),
  spell('whiteout', 'Whiteout', 'ice', 'column', 3, 'Freeze a lane, scatter cutting ice shards and push enemies back.', ['ice', 'wind'], 1),
  spell('sandstorm', 'Sandstorm', 'earth', 'area', 4, 'Blind enemies for 2 rounds, drive them back and leave stone cover.', ['wind', 'earth'], 1),
  spell('inferno', 'Inferno', 'fire', 'area', 7, 'Heavy fire damage across a 3×3 area; leave 3 rounds of flame.', ['fire'], 1),
  spell('flood', 'Great Flood', 'water', 'twoColumns', 4, 'Flood two distinct lanes, clear harmful ground, and push enemies back.', ['water'], 1),
  spell('worldroot', 'Worldroot', 'nature', 'largeArea', 3, 'Root a wide area for 3 rounds and mend the tower by 5.', ['nature'], 1),
  spell('chain', 'Chain Lightning', 'lightning', 'chain', 8, 'A powerful arc through linked enemies and puddles; soaked foes are stunned.', ['lightning'], 1),
  spell('permafrost', 'Permafrost', 'ice', 'column', 5, 'Freeze an entire lane for 3 rounds; wet targets stay frozen longer.', ['ice'], 1),
  spell('bastion', 'Bastion', 'earth', 'row', 6, 'Create a strong barricade across a row. Repair the tower by 5.', ['earth'], 1),
  spell('cyclone', 'Cyclone', 'wind', 'area', 5, 'Push an area back 2 tiles and scatter its elemental terrain.', ['wind'], 1)
].map(s => [s.key, s]));

export const RECIPES = [
  ['fire', 'water', 'steam'], ['fire', 'nature', 'wildfire'], ['fire', 'lightning', 'plasma'],
  ['fire', 'ice', 'thermal'], ['fire', 'earth', 'eruption'], ['fire', 'wind', 'firestorm'],
  ['water', 'nature', 'overgrowth'], ['water', 'lightning', 'tempest'], ['water', 'ice', 'blizzard'],
  ['water', 'earth', 'mire'], ['water', 'wind', 'tidal'], ['nature', 'lightning', 'stormroot'],
  ['nature', 'ice', 'frostbloom'], ['nature', 'earth', 'briarwall'], ['nature', 'wind', 'spores'],
  ['lightning', 'ice', 'hailstorm'], ['lightning', 'earth', 'lodestone'], ['lightning', 'wind', 'thundercloud'],
  ['ice', 'earth', 'glacier'], ['ice', 'wind', 'whiteout'], ['earth', 'wind', 'sandstorm'],
  ['fire', 'fire', 'inferno'], ['water', 'water', 'flood'], ['nature', 'nature', 'worldroot'],
  ['lightning', 'lightning', 'chain'], ['ice', 'ice', 'permafrost'], ['earth', 'earth', 'bastion'],
  ['wind', 'wind', 'cyclone']
].map(([a, b, key]) => ({ a, b, key, name: SPELLS[key].name }));
const recipeMap = new Map(RECIPES.map(r => [[r.a, r.b].sort().join('+'), r.key]));
export function getRecipe(a, b) { return recipeMap.get([a, b].sort().join('+')) || null; }

export const INTERACTIONS = [
  { key: 'conduction', name: 'Conducted', when: 'Lightning + puddles / wet foes', description: '+3 damage and 1-round stun. Electricity follows every connected puddle.' },
  { key: 'bramble', name: 'Bramble blaze', when: 'Fire + vines', description: '+3 impact damage; consumes vines to leave 3-damage flames for 3 rounds.' },
  { key: 'freeze', name: 'Flash freeze', when: 'Ice + water', description: 'Freeze soaked foes for 2 rounds and leave longer-lasting ice.' },
  { key: 'thermal', name: 'Thermal shock', when: 'Fire + ice / frozen foes', description: '+4 damage; shatter the freeze into blinding steam.' },
  { key: 'steam', name: 'Steam burst', when: 'Fire + water', description: '+2 damage; evaporate the puddle into steam that suppresses ranged abilities.' },
  { key: 'douse', name: 'Doused', when: 'Water + fire', description: 'Remove flames and burning from foes, leaving steam and a fresh puddle.' },
  { key: 'growth', name: 'Overgrowth', when: 'Water + vines / nature + water', description: 'Vines last an extra round and spread to adjacent path tiles.' },
  { key: 'mud', name: 'Quagmire', when: 'Water + stone / earth + water', description: 'Leave deep mud for 3 rounds; moving through it costs an extra step.' },
  { key: 'fanned', name: 'Fanned flames', when: 'Wind + fire', description: 'Push foes back and spread fire into neighboring tiles.' },
  { key: 'spray', name: 'Spreading spray', when: 'Wind + water', description: 'Spread puddles sideways, connecting a larger lightning circuit.' },
  { key: 'shatter', name: 'Shattered', when: 'Earth + ice / frozen foes', description: '+4 impact damage and 2 damage to adjacent foes; consumes the freeze.' },
  { key: 'combustion', name: 'Spore explosion', when: 'Fire + poison', description: '+4 damage and 2 splash damage; consumes poison.' },
  { key: 'sporeWind', name: 'Spore cloud', when: 'Wind + poison', description: 'Spread poison to adjacent tiles and foes.' },
  { key: 'roots', name: 'Living circuit', when: 'Lightning + vines', description: '+2 damage and stun rooted foes for a round.' },
  { key: 'mist', name: 'Charged mist', when: 'Lightning + steam', description: '+2 damage and 1-round stun; consume the steam.' },
  { key: 'fuel', name: 'Fresh fuel', when: 'Nature + fire', description: 'Feed existing fire: +1 burn damage and an extra round of flames.' },
  { key: 'frostRoot', name: 'Frost snare', when: 'Ice + vines / nature + ice', description: 'Freeze and root together: movement remains blocked after one effect ends.' },
  { key: 'molten', name: 'Molten rock', when: 'Fire + stone / earth + fire', description: '+2 impact damage and 3-damage flames; consumes the stone.' },
  { key: 'cleanse', name: 'Cleansing rain', when: 'Water + poison', description: 'Wash away poison from the ground and enemies.' },
  { key: 'splinters', name: 'Ice splinters', when: 'Wind + ice', description: '+2 damage and scatter frost to adjacent tiles.' },
  { key: 'frozenMud', name: 'Frozen mire', when: 'Ice + mud', description: 'Freeze the mud into ice for 4 rounds and freeze occupants for 2.' },
  { key: 'rampart', name: 'Briar rampart', when: 'Earth + vines', description: 'Reinforce the root patch with a stronger, 12-health barricade.' },
  { key: 'grounded', name: 'Grounded', when: 'Lightning + stone', description: 'Stone absorbs 2 lightning damage: remove or flood a barricade before shocking through it.' }
];

export const ENEMIES = {
  rat: { key: 'rat', name: 'Cave Rat', hp: 6, speed: 2, damage: 2, weak: ['fire'], resist: [], wave: 1 },
  imp: { key: 'imp', name: 'Ember Imp', hp: 9, speed: 2, damage: 3, weak: ['water', 'ice'], resist: ['fire'], wave: 2 },
  orc: { key: 'orc', name: 'Moss Orc', hp: 13, speed: 1, damage: 4, weak: ['lightning'], resist: ['earth'], wave: 1 },
  troll: { key: 'troll', name: 'Bramble Troll', hp: 23, speed: 1, damage: 5, weak: ['fire'], resist: ['nature'], wave: 3 },
  warg: { key: 'warg', name: 'Moon Warg', hp: 15, speed: 2, damage: 3, weak: ['ice', 'lightning'], resist: [], wave: 3 },
  mage: { key: 'mage', name: 'Hollow Mage', hp: 18, speed: 1, damage: 4, weak: ['nature'], resist: ['ice'], wave: 4, ability: 'bolt' },
  golem: { key: 'golem', name: 'Stone Golem', hp: 29, speed: 1, damage: 6, weak: ['water', 'nature'], resist: ['earth', 'lightning'], wave: 5, immovable: true },
  slime: { key: 'slime', name: 'Spore Slime', hp: 12, speed: 1, damage: 3, weak: ['lightning', 'ice'], resist: ['nature'], wave: 2, ability: 'split' },
  baby: { key: 'baby', name: 'Slimelet', hp: 4, speed: 1, damage: 1, weak: ['fire'], resist: [], wave: 99 },
  specter: { key: 'specter', name: 'Mist Wraith', hp: 14, speed: 2, damage: 4, weak: ['fire', 'lightning'], resist: ['water', 'nature'], wave: 6, rootImmune: true },
  ogre: { key: 'ogre', name: 'Thorncrown Ogre', hp: 88, speed: 1, damage: 12, weak: ['fire', 'lightning'], resist: ['nature'], wave: 5, boss: true },
  dragon: { key: 'dragon', name: 'The Ashwing', hp: 155, speed: 1, damage: 25, weak: ['water', 'ice'], resist: ['fire'], wave: 10, boss: true, ability: 'breath' }
};

const clone = value => JSON.parse(JSON.stringify(value));
const inBoard = (x, y) => Number.isInteger(x) && Number.isInteger(y) && x >= 0 && x < COLS && y >= 0 && y < TOWER_ROW;
const keyOf = (x, y) => `${x},${y}`;
const unique = cells => [...new Map(cells.filter(p => inBoard(p.x, p.y)).map(p => [keyOf(p.x, p.y), p])).values()];
const around = (x, y) => unique([{ x, y: y - 1 }, { x: x + 1, y }, { x, y: y + 1 }, { x: x - 1, y }]);
const at = (d, x, y) => d.enemies.filter(e => e.hp > 0 && e.x === x && e.y === y);
const ground = (d, x, y) => d.terrain[keyOf(x, y)] || (d.terrain[keyOf(x, y)] = {});
const active = (tile, field) => (tile?.[field] || 0) > 0;
const setStatus = (e, status, duration) => { e[status] = Math.max(e[status] || 0, duration); };
const nextId = (d, prefix = 'e') => `${prefix}${++d.idCounter}`;
function rng(d) {
  d.rng = (d.rng + 0x6D2B79F5) >>> 0;
  let t = d.rng;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
function seedNumber(seed) {
  if (Number.isFinite(seed)) return Number(seed) >>> 0;
  let value = 2166136261;
  for (const char of String(seed ?? 'spelltower')) value = Math.imul(value ^ char.charCodeAt(0), 16777619) >>> 0;
  return value;
}
function emit(d, type, text, x = -1, y = -1, extra = {}) {
  d.events.push({ id: nextId(d, 'fx'), type, text, element: '', x, y, amount: 0, ...extra });
  // Keep the spell's footprint available to remote animation and target previews even in a busy combo.
  if (d.events.length > 72) d.events.splice(Math.max(0, d.events.findIndex(e => e.type !== 'cast')), 1);
}
function combo(d, name, x, y, element, amount = 0) {
  emit(d, 'combo', name, x, y, { element, amount });
}
function makeCard(d, key, charges = SPELLS[key].charges) {
  return { id: nextId(d, 'card'), key, charges, usedTurn: -1 };
}
function seedHand(d) { return BASE_ELEMENTS.map(key => makeCard(d, key)); }
export function getActivePlayer(state) { return state.players[state.activeIndex] || ''; }
const validPlayer = player => typeof player === 'string' && player.trim().length > 0 && player.length <= 32
  && !/[\u0000-\u001f\u007f]/.test(player) && !['__proto__', 'constructor', 'prototype'].includes(player.toLowerCase());

export function createGame({ players = ['Mage'], mode = 'solo', seed = 'spelltower', difficulty = 'normal' } = {}) {
  if (!['solo', 'hotseat', 'online'].includes(mode)) throw new Error('Unknown game mode.');
  if (!Array.isArray(players) || !players.length || players.length > MAX_PLAYERS || players.some(p => !validPlayer(p)) || new Set(players).size !== players.length) throw new Error('Choose unique player names of 1–32 characters.');
  if (mode === 'solo' && players.length !== 1) throw new Error('Solo games have one player.');
  if (mode === 'hotseat' && players.length > 4) throw new Error('Hot-seat games have up to four players.');
  const level = typeof difficulty === 'number' ? ['easy', 'normal', 'hard'][Math.max(0, Math.min(2, difficulty - 1))] : difficulty;
  if (!DIFFICULTIES[level]) throw new Error('Unknown difficulty.');
  const config = DIFFICULTIES[level];
  const d = {
    version: 2, mode, difficulty: level, players: [...players], hands: {}, activeIndex: 0,
    pendingPlayers: [...players], turn: 0, round: 1, wave: 1, roundsInWave: 1,
    hp: config.hp, maxHP: config.hp, mana: config.mana, maxMana: config.mana,
    enemies: [], spawnQueue: [], terrain: {}, status: 'playing', events: [], revision: 0,
    rng: seedNumber(seed), idCounter: 0, kills: 0, combos: 0
  };
  for (const player of players) d.hands[player] = seedHand(d);
  spawnGroup(d);
  emit(d, 'wave', 'Wave 1 · Defend the tower');
  return d;
}

function shape(pattern, x, y) {
  const cells = [];
  if (pattern === 'column') for (let yy = 0; yy < TOWER_ROW; yy++) cells.push({ x, y: yy });
  else if (pattern === 'twoColumns') {
    const other = x === COLS - 1 ? x - 1 : x + 1;
    for (let yy = 0; yy < TOWER_ROW; yy++) cells.push({ x, y: yy }, { x: other, y: yy });
  } else if (pattern === 'row') for (let xx = 0; xx < COLS; xx++) cells.push({ x: xx, y });
  else if (pattern === 'cross') cells.push({ x, y }, ...around(x, y));
  else if (pattern === 'area' || pattern === 'largeArea') {
    const radius = pattern === 'largeArea' ? 2 : 1;
    for (let yy = y - radius; yy <= y + radius; yy++) for (let xx = x - radius; xx <= x + radius; xx++) cells.push({ x: xx, y: yy });
  } else cells.push({ x, y });
  return unique(cells);
}

function conductionCells(d, seeds) {
  const seen = new Map();
  const queue = [...seeds];
  while (queue.length) {
    const p = queue.shift(), key = keyOf(p.x, p.y);
    if (seen.has(key) || !inBoard(p.x, p.y)) continue;
    seen.set(key, p);
    const conducting = active(d.terrain[key], 'wet') || at(d, p.x, p.y).length > 0;
    if (!conducting) continue;
    for (const n of around(p.x, p.y)) {
      if (active(d.terrain[keyOf(n.x, n.y)], 'wet') || at(d, n.x, n.y).length) queue.push(n);
    }
  }
  return [...seen.values()];
}

function hurt(d, enemy, amount, element = 'neutral') {
  if (enemy.hp <= 0 || amount <= 0) return;
  const spec = ENEMIES[enemy.key];
  const mult = (spec.weak.includes(element) ? 1.4 : 1) * (spec.resist.includes(element) ? 0.65 : 1) * (enemy.vulnerable > 0 ? 1.25 : 1);
  const damage = Math.max(1, Math.round(amount * mult));
  enemy.hp = Math.max(0, enemy.hp - damage);
  emit(d, 'damage', `−${damage}`, enemy.x, enemy.y, { element, amount: damage });
}
function damageCell(d, x, y, amount, element) { for (const e of at(d, x, y)) hurt(d, e, amount, element); }
function splash(d, x, y, amount, element) { for (const p of around(x, y)) damageCell(d, p.x, p.y, amount, element); }
function spread(d, x, y, field, duration, extra = {}) {
  for (const p of around(x, y)) {
    const t = ground(d, p.x, p.y);
    t[field] = Math.max(t[field] || 0, duration);
    Object.assign(t, extra);
    for (const e of at(d, p.x, p.y)) {
      if (field === 'wet') setStatus(e, 'wet', duration);
      if (field === 'poison') setStatus(e, 'poison', duration);
    }
  }
}
function push(d, e, amount) {
  if (e.hp <= 0 || ENEMIES[e.key].immovable) return;
  const start = e.y;
  for (let i = 0; i < amount && e.y > 0; i++) {
    if (at(d, e.x, e.y - 1).some(other => other.id !== e.id) || active(d.terrain[keyOf(e.x, e.y - 1)], 'stone')) break;
    e.y--;
  }
  if (e.y !== start) emit(d, 'move', 'Pushed back', e.x, e.y, { element: 'wind', amount: start - e.y });
}

function applyElement(d, element, cells, damage, options = {}) {
  const duration = options.duration || 2;
  const affected = element === 'lightning' ? conductionCells(d, cells) : cells;
  // Take a terrain snapshot: a gust spreads each original patch once, not recursively across the map.
  const before = clone(d.terrain);
  const pushed = new Set();
  for (const { x, y } of affected) {
    const tile = ground(d, x, y), original = before[keyOf(x, y)] || {};
    const enemies = at(d, x, y);
    let bonus = 0;
    const any = status => enemies.some(e => active(e, status));
    const announce = (name, amount = 0) => combo(d, name, x, y, element, amount);
    if (element === 'fire') {
      const frozen = active(tile, 'ice') || any('frozen');
      if (frozen) {
        bonus += 4; tile.ice = 0; tile.steam = 2;
        for (const e of enemies) { e.frozen = 0; e.blinded = Math.max(e.blinded || 0, 1); }
        announce('Thermal shock +4', 4);
      } else if (active(tile, 'wet') || any('wet')) {
        bonus += 2; tile.steam = 2;
        for (const e of enemies) setStatus(e, 'blinded', 1);
        announce('Steam burst +2', 2);
      }
      tile.wet = 0;
      for (const e of enemies) e.wet = 0;
      if (active(tile, 'vines') || any('rooted')) {
        bonus += 3; tile.vines = 0; tile.burn = 3; tile.burnPower = 3;
        for (const e of enemies) { e.rooted = 0; setStatus(e, 'burn', 3); e.burnPower = 3; }
        announce('Bramble blaze +3', 3);
      }
      if (active(tile, 'poison') || any('poison')) {
        bonus += 4; tile.poison = 0;
        for (const e of enemies) e.poison = 0;
        splash(d, x, y, 2, 'fire'); announce('Spore explosion +4', 4);
      }
      if (active(tile, 'stone')) {
        bonus += 2; tile.stone = 0; tile.strength = 0; tile.burn = 3; tile.burnPower = 3;
        announce('Molten rock +2', 2);
      }
      if (options.lingering || bonus >= 3) {
        tile.burn = Math.max(tile.burn || 0, options.lingering || 2);
        tile.burnPower = Math.max(tile.burnPower || 0, 2);
        for (const e of enemies) { setStatus(e, 'burn', tile.burn); e.burnPower = tile.burnPower; }
      }
    } else if (element === 'water') {
      if (active(tile, 'burn') || any('burn')) {
        tile.burn = 0; tile.burnPower = 0; tile.steam = 2;
        for (const e of enemies) { e.burn = 0; e.burnPower = 0; setStatus(e, 'blinded', 1); }
        announce('Doused · flames removed');
      }
      if (active(tile, 'poison') || any('poison')) {
        tile.poison = 0; for (const e of enemies) e.poison = 0;
        announce('Cleansing rain');
      }
      if (active(tile, 'vines')) {
        tile.vines = Math.min(5, tile.vines + 1); spread(d, x, y, 'vines', 2);
        announce('Overgrowth · vines spread');
      }
      if (active(tile, 'stone')) {
        tile.stone = 0; tile.strength = 0; tile.mud = 3;
        for (const e of enemies) setStatus(e, 'slow', 2);
        announce('Quagmire · slowed');
      }
      tile.wet = Math.max(tile.wet || 0, 3);
      for (const e of enemies) setStatus(e, 'wet', 3);
    } else if (element === 'nature') {
      if (active(tile, 'burn')) {
        tile.burn = Math.min(5, tile.burn + 1); tile.burnPower = Math.min(4, (tile.burnPower || 2) + 1);
        announce('Fresh fuel · stronger fire');
      } else {
        let roots = duration;
        if (active(tile, 'wet') || any('wet')) {
          roots++; spread(d, x, y, 'vines', 2); announce('Overgrowth · vines spread');
        }
        tile.vines = Math.max(tile.vines || 0, roots);
        for (const e of enemies) if (!ENEMIES[e.key].rootImmune) setStatus(e, 'rooted', roots);
      }
      if (active(tile, 'ice')) {
        for (const e of enemies) { setStatus(e, 'frozen', 1); if (!ENEMIES[e.key].rootImmune) setStatus(e, 'rooted', 2); }
        announce('Frost snare');
      }
    } else if (element === 'lightning') {
      if (active(tile, 'wet') || any('wet')) {
        bonus += 3; tile.charged = 1;
        for (const e of enemies) setStatus(e, 'stun', 1);
        announce('Conducted +3 · stunned', 3);
      }
      if (active(tile, 'vines') || any('rooted')) {
        bonus += 2; for (const e of enemies) setStatus(e, 'stun', 1);
        announce('Living circuit +2', 2);
      }
      if (active(tile, 'steam')) {
        bonus += 2; tile.steam = 0; tile.charged = 1;
        for (const e of enemies) setStatus(e, 'stun', 1);
        announce('Charged mist +2', 2);
      }
      if (active(tile, 'stone')) { bonus -= 2; announce('Grounded −2', -2); }
      if (options.stun) for (const e of enemies) setStatus(e, 'stun', 1);
    } else if (element === 'ice') {
      let freeze = 1, iceDuration = duration;
      if (active(tile, 'wet') || any('wet')) {
        freeze = 2; iceDuration = Math.max(iceDuration, 3); tile.wet = 0;
        for (const e of enemies) e.wet = 0;
        announce('Flash freeze · 2 rounds');
      }
      if (active(tile, 'burn')) {
        tile.burn = 0; tile.burnPower = 0; tile.steam = 2; bonus += 2;
        for (const e of enemies) { e.burn = 0; e.burnPower = 0; }
        announce('Thermal shock +2', 2);
      }
      if (active(tile, 'mud')) { tile.mud = 0; freeze = 2; iceDuration = 4; announce('Frozen mire'); }
      if (active(tile, 'vines')) {
        for (const e of enemies) if (!ENEMIES[e.key].rootImmune) setStatus(e, 'rooted', 2);
        announce('Frost snare');
      }
      tile.ice = Math.max(tile.ice || 0, iceDuration);
      for (const e of enemies) setStatus(e, 'frozen', freeze);
    } else if (element === 'earth') {
      let placeStone = true;
      if (active(tile, 'ice') || any('frozen')) {
        bonus += 4; tile.ice = 0;
        for (const e of enemies) e.frozen = 0;
        splash(d, x, y, 2, 'earth'); announce('Shattered +4', 4);
      }
      if (active(tile, 'wet') || any('wet')) {
        tile.mud = 3; tile.wet = 0; placeStone = false;
        for (const e of enemies) { e.wet = 0; setStatus(e, 'slow', 2); }
        announce('Quagmire · slowed');
      }
      if (active(tile, 'burn')) {
        bonus += 2; tile.burnPower = 3; tile.burn = Math.max(tile.burn, 3); placeStone = false;
        announce('Molten rock +2', 2);
      }
      if (placeStone) {
        tile.stone = Math.max(tile.stone || 0, duration); tile.strength = options.strength || 8;
        if (active(tile, 'vines')) { tile.strength = Math.max(tile.strength, 12); announce('Briar rampart'); }
      }
    } else if (element === 'wind') {
      if (active(original, 'burn')) { spread(d, x, y, 'burn', 2, { burnPower: original.burnPower || 2 }); announce('Fanned flames'); }
      if (active(original, 'wet')) { spread(d, x, y, 'wet', 2); announce('Spreading spray'); }
      if (active(original, 'poison')) { spread(d, x, y, 'poison', 3); announce('Spore cloud'); }
      if (active(original, 'ice')) { bonus += 2; spread(d, x, y, 'ice', 1); announce('Ice splinters +2', 2); }
    }
    for (const e of enemies) hurt(d, e, Math.max(0, damage + bonus), element);
    if (element === 'wind') {
      // Process each foe only once even if its new position remains inside this spell's area.
      for (const e of enemies) if (!pushed.has(e.id)) { pushed.add(e.id); push(d, e, options.push || 1); }
    }
  }
  return affected;
}

function executeSpell(d, spec, x, y) {
  let cells = shape(spec.pattern, x, y);
  if (spec.pattern === 'chain') cells = conductionCells(d, cells);
  const fx = { element: spec.element, cells: clone(cells) };
  emit(d, 'cast', spec.name, x, y, fx);
  const allCells = [...cells];
  const apply = (element, damage, options = {}) => allCells.push(...applyElement(d, element, cells, damage, options));
  if (spec.key === 'spores') {
    for (const p of cells) {
      ground(d, p.x, p.y).poison = 3;
      for (const e of at(d, p.x, p.y)) setStatus(e, 'poison', 3);
    }
    apply('nature', spec.damage); apply('wind', 1);
  } else if (spec.key === 'steam') {
    apply('water', 1); apply('fire', spec.damage);
    for (const p of cells) ground(d, p.x, p.y).steam = 3;
  } else if (spec.key === 'lodestone') {
    for (const p of cells) for (const e of at(d, p.x, p.y)) setStatus(e, 'vulnerable', 2);
    apply('lightning', spec.damage); apply('earth', 1);
  } else if (spec.key === 'sandstorm') {
    for (const p of cells) for (const e of at(d, p.x, p.y)) setStatus(e, 'blinded', 2);
    apply('wind', spec.damage); apply('earth', 1);
  } else if (spec.parts.length > 1) {
    const [first, second] = spec.parts;
    const setupDamage = ['fire', 'lightning'].includes(first) ? spec.damage : 1;
    apply(first, setupDamage, { lingering: first === 'fire' ? 2 : 0 });
    apply(second, ['wind', 'nature', 'water', 'ice', 'earth'].includes(first) ? spec.damage : 2, {
      lingering: second === 'fire' ? 3 : 0, push: spec.key === 'tidal' ? 2 : 1,
      stun: spec.key === 'hailstorm'
    });
  } else {
    apply(spec.element, spec.damage, {
      duration: ['worldroot', 'permafrost', 'bastion'].includes(spec.key) ? 3 : 2,
      lingering: spec.key === 'inferno' ? 3 : 0,
      push: spec.key === 'cyclone' ? 2 : 1,
      strength: spec.key === 'bastion' ? 16 : 8
    });
    if (spec.key === 'flood') for (const e of [...d.enemies].sort((a, b) => a.y - b.y)) if (cells.some(p => p.x === e.x && p.y === e.y)) push(d, e, 1);
  }
  if (['worldroot', 'bastion', 'overgrowth'].includes(spec.key)) {
    const healed = Math.min(d.maxHP - d.hp, spec.key === 'overgrowth' ? 3 : 5);
    d.hp += healed;
    if (healed) emit(d, 'heal', `Tower +${healed}`, x, TOWER_ROW, { element: 'nature', amount: healed });
  }
  const castEvent = d.events.find(e => e.type === 'cast');
  if (castEvent) castEvent.cells = unique(allCells);
  cleanupDead(d);
  compactTerrain(d);
}

export function previewSpell(state, spellKey, x, y) {
  const spec = SPELLS[spellKey];
  if (!spec || !inBoard(x, y)) return { cells: [], text: 'Choose a path tile above the tower.', combos: [] };
  const d = clone(state); d.events = [];
  executeSpell(d, spec, x, y);
  const cells = d.events.find(e => e.type === 'cast')?.cells || shape(spec.pattern, x, y);
  const combos = [...new Set(d.events.filter(e => e.type === 'combo').map(e => e.text))];
  return { cells, text: `${spec.name} · ${spec.cost} mana · ${spec.description}`, combos };
}

function spawnEnemy(d, key, x, y = 0, event = true) {
  if (!inBoard(x, y) || at(d, x, y).length) return false;
  const spec = ENEMIES[key];
  const party = 1 + 0.18 * Math.max(0, d.players.length - 1);
  const hp = Math.round(spec.hp * DIFFICULTIES[d.difficulty].scale * (spec.boss ? 1 : 1 + (d.wave - 1) * 0.045) * party);
  d.enemies.push({ id: nextId(d), key, x, y, hp, maxHP: hp, speed: spec.speed, damage: spec.damage,
    wet: 0, stun: 0, frozen: 0, rooted: 0, burn: 0, burnPower: 0, poison: 0, slow: 0, blinded: 0, vulnerable: 0 });
  if (event) emit(d, 'spawn', spec.name, x, y, { element: 'shadow' });
  return true;
}
function spawnGroup(d) {
  const count = Math.min(COLS, 1 + Math.ceil(d.wave / 3) + Math.floor((d.players.length - 1) / 3));
  d.spawnQueue ||= [];
  if (d.roundsInWave === 1 && [5, LAST_WAVE].includes(d.wave)) d.spawnQueue.push({ key: d.wave === LAST_WAVE ? 'dragon' : 'ogre' });
  const pool = Object.values(ENEMIES).filter(s => !s.boss && s.wave <= d.wave);
  for (let i = 0; i < count; i++) {
    const spec = pool[Math.floor(rng(d) * pool.length)];
    d.spawnQueue.push({ key: spec.key });
  }
  drainSpawns(d);
}
function drainSpawns(d) {
  const available = [...Array(COLS).keys()].filter(x => !at(d, x, 0).length);
  while (d.spawnQueue?.length && available.length) {
    const x = available.splice(Math.floor(rng(d) * available.length), 1)[0];
    spawnEnemy(d, d.spawnQueue.shift().key, x);
  }
}
function cleanupDead(d) {
  const dead = d.enemies.filter(e => e.hp <= 0);
  d.enemies = d.enemies.filter(e => e.hp > 0);
  for (const e of dead) {
    d.kills++;
    emit(d, 'death', ENEMIES[e.key].name, e.x, e.y, { element: 'shadow' });
    if (e.key === 'slime') {
      let spawned = 0;
      for (const p of [{ x: e.x, y: e.y }, ...around(e.x, e.y)]) {
        if (spawned === 2) break;
        if (spawnEnemy(d, 'baby', p.x, p.y)) spawned++;
      }
    }
  }
}

const timerKeys = ['wet', 'vines', 'burn', 'ice', 'mud', 'stone', 'steam', 'charged', 'poison'];
const enemyTimers = ['wet', 'stun', 'frozen', 'rooted', 'burn', 'poison', 'slow', 'blinded', 'vulnerable'];
function compactTerrain(d) {
  for (const [key, t] of Object.entries(d.terrain)) {
    for (const field of timerKeys) if (!active(t, field)) delete t[field];
    if (!t.burn) delete t.burnPower;
    if (!t.stone) delete t.strength;
    if (!timerKeys.some(field => active(t, field))) delete d.terrain[key];
  }
}
function surface(d, e, entering = false) {
  const t = d.terrain[keyOf(e.x, e.y)] || {};
  if (active(t, 'wet')) setStatus(e, 'wet', 2);
  if (active(t, 'vines') && !ENEMIES[e.key].rootImmune) setStatus(e, 'rooted', 1);
  // Standing on old ice does not continually renew a spell's explicit freeze duration.
  if (entering && active(t, 'ice')) setStatus(e, 'frozen', 1);
  if (active(t, 'mud')) setStatus(e, 'slow', 1);
  if (active(t, 'steam')) setStatus(e, 'blinded', 1);
  if (active(t, 'poison')) setStatus(e, 'poison', 2);
  if (active(t, 'burn')) { setStatus(e, 'burn', 1); e.burnPower = Math.max(e.burnPower || 0, t.burnPower || 2); }
  if (entering && active(t, 'burn')) hurt(d, e, t.burnPower || 2, 'fire');
  if (entering && active(t, 'charged')) { hurt(d, e, 2, 'lightning'); setStatus(e, 'stun', 1); }
}

function advanceRound(d) {
  // Damage and crowd control happen once after all participating teammates have acted.
  for (const e of [...d.enemies]) {
    surface(d, e);
    if (e.burn > 0) hurt(d, e, e.burnPower || 2, 'fire');
    if (e.poison > 0) hurt(d, e, 2, 'nature');
  }
  cleanupDead(d);
  for (const e of [...d.enemies].sort((a, b) => b.y - a.y)) {
    if (e.hp <= 0) continue;
    const spec = ENEMIES[e.key];
    const disabled = e.stun > 0 || e.frozen > 0;
    if (!disabled && e.blinded <= 0) {
      if (spec.ability === 'bolt' && d.round % 3 === 0) {
        d.hp = Math.max(0, d.hp - 2); emit(d, 'tower', 'Hollow bolt −2', e.x, TOWER_ROW, { element: 'shadow', amount: 2 });
      }
      if (spec.ability === 'breath' && d.round % 3 === 0) {
        d.hp = Math.max(0, d.hp - 4); emit(d, 'tower', 'Dragon breath −4', e.x, TOWER_ROW, { element: 'fire', amount: 4 });
      }
    }
    if (disabled || (e.rooted > 0 && !spec.rootImmune)) continue;
    let steps = e.speed;
    if (e.slow > 0) steps = Math.max(0, steps - 1);
    while (steps-- > 0 && e.hp > 0) {
      const ny = e.y + 1;
      if (ny >= TOWER_ROW) {
        d.hp = Math.max(0, d.hp - e.damage); e.escaped = true;
        emit(d, 'tower', `Tower −${e.damage}`, e.x, TOWER_ROW, { element: 'shadow', amount: e.damage });
        break;
      }
      if (at(d, e.x, ny).some(other => other.id !== e.id)) break;
      const next = d.terrain[keyOf(e.x, ny)] || {};
      if (active(next, 'stone')) {
        next.strength = Math.max(0, (next.strength || 8) - e.damage);
        if (next.strength === 0) next.stone = 0;
        emit(d, 'status', 'Barricade blocks', e.x, ny, { element: 'earth' });
        break;
      }
      e.y = ny; surface(d, e, true);
      if (e.stun > 0 || e.frozen > 0 || (e.rooted > 0 && !spec.rootImmune) || e.slow > 0) break;
    }
  }
  d.enemies = d.enemies.filter(e => !e.escaped);
  cleanupDead(d);
  for (const e of d.enemies) for (const key of enemyTimers) e[key] = Math.max(0, (e[key] || 0) - 1);
  for (const t of Object.values(d.terrain)) for (const key of timerKeys) if (active(t, key)) t[key]--;
  compactTerrain(d);
  d.round++;
  if (d.hp <= 0) {
    d.status = 'lost'; emit(d, 'defeat', `The tower fell on wave ${d.wave}`); return;
  }
  drainSpawns(d);
  if (d.roundsInWave < 3) { d.roundsInWave++; spawnGroup(d); }
  else if (d.enemies.length === 0 && !d.spawnQueue?.length) {
    if (d.wave === LAST_WAVE) { d.status = 'won'; emit(d, 'victory', 'The Ashwing is defeated. The tower stands!'); return; }
    d.wave++; d.roundsInWave = 1;
    const healed = Math.min(3, d.maxHP - d.hp); d.hp += healed;
    emit(d, 'wave', `Wave ${d.wave} · ${[5, LAST_WAVE].includes(d.wave) ? 'Boss incoming' : 'The next wave approaches'}`);
    if (healed) emit(d, 'heal', `Wave cleared · Tower +${healed}`, -1, TOWER_ROW, { amount: healed, element: 'nature' });
    spawnGroup(d);
  }
}

function refreshHand(d, player) {
  const hand = d.hands[player];
  for (const key of BASE_ELEMENTS) if (hand.length < MAX_HAND && !hand.some(c => c.key === key)) hand.push(makeCard(d, key, 2));
  const bases = hand.filter(c => BASE_ELEMENTS.includes(c.key) && c.charges < 3).sort((a, b) => a.charges - b.charges || a.key.localeCompare(b.key));
  for (const card of bases.slice(0, 2)) card.charges++;
  // One duplicate each round opens self-fusion recipes without requiring a second device.
  if (hand.length < MAX_HAND) {
    const key = BASE_ELEMENTS[(d.round - 1) % BASE_ELEMENTS.length];
    if (hand.filter(c => c.key === key).length < 2) hand.push(makeCard(d, key, 2));
  }
}
function nextPending(d, startIndex) {
  for (let step = 1; step <= d.players.length; step++) {
    const index = (startIndex + step + d.players.length) % d.players.length;
    if (d.pendingPlayers.includes(d.players[index])) return index;
  }
  return 0;
}
function endTurn(d, actor) {
  refreshHand(d, actor);
  d.pendingPlayers = d.pendingPlayers.filter(p => p !== actor);
  d.turn++;
  if (!d.pendingPlayers.length) {
    advanceRound(d);
    d.pendingPlayers = [...d.players];
    d.activeIndex = 0;
  } else d.activeIndex = nextPending(d, d.activeIndex);
  d.mana = d.maxMana;
  emit(d, 'turn', d.status === 'playing' ? `${getActivePlayer(d)}'s turn` : 'Run complete');
}

export function applyAction(state, actor, action) {
  if (!state || state.version !== 2 || !action || typeof action.type !== 'string') throw new Error('Invalid game action.');
  if (typeof actor !== 'string' || !actor) throw new Error('A player name is required.');
  const d = clone(state); d.events = [];
  if (action.type === 'join') {
    const player = action.player;
    if (actor !== player || !validPlayer(player)) throw new Error('Join with your own valid player name (1–32 characters).');
    if (d.status !== 'playing') throw new Error('This run has ended.');
    if (d.mode !== 'online') throw new Error('Local seats are selected before the run.');
    if (d.players.includes(player)) throw new Error('You already joined this run.');
    if (d.players.length >= MAX_PLAYERS) throw new Error('This party is full.');
    d.players.push(player); d.hands[player] = seedHand(d);
    emit(d, 'join', `${player} joins next round`);
  } else if (action.type === 'leave') {
    if (action.player !== actor || !d.players.includes(actor)) throw new Error('You can only leave your own seat.');
    const oldActive = getActivePlayer(d), removed = d.players.indexOf(actor);
    d.players.splice(removed, 1); d.pendingPlayers = d.pendingPlayers.filter(p => p !== actor); delete d.hands[actor];
    if (!d.players.length) { d.activeIndex = 0; d.status = 'lost'; emit(d, 'defeat', 'The party left the tower.'); }
    else if (d.status === 'playing' && !d.pendingPlayers.length) {
      advanceRound(d); d.pendingPlayers = [...d.players]; d.activeIndex = 0; d.turn++; d.mana = d.maxMana;
    } else if (oldActive === actor) { d.activeIndex = nextPending(d, removed - 1); d.turn++; d.mana = d.maxMana; }
    else d.activeIndex = d.players.indexOf(oldActive);
    emit(d, 'leave', `${actor} left the party`);
  } else {
    if (d.status !== 'playing') throw new Error('This run has ended.');
    if (!d.players.includes(actor) || actor !== getActivePlayer(d)) throw new Error('Wait for your turn.');
    const hand = d.hands[actor];
    if (action.type === 'cast') {
      const card = hand.find(c => c.id === action.cardId);
      if (!card || card.charges < 1) throw new Error('That spell is no longer in your hand.');
      if (card.usedTurn === d.turn) throw new Error('Each card can be cast once per turn.');
      if (!inBoard(action.x, action.y)) throw new Error('Choose a path tile above the tower.');
      const spec = SPELLS[card.key];
      if (d.mana < spec.cost) throw new Error('Not enough mana. End your turn to refill.');
      d.mana -= spec.cost; card.charges--; card.usedTurn = d.turn;
      executeSpell(d, spec, action.x, action.y);
      if (card.charges === 0) d.hands[actor] = hand.filter(c => c.id !== card.id);
    } else if (action.type === 'combine') {
      if (!Array.isArray(action.cardIds) || action.cardIds.length !== 2 || action.cardIds[0] === action.cardIds[1]) throw new Error('Choose two different cards.');
      const cards = action.cardIds.map(id => hand.find(c => c.id === id));
      if (cards.some(c => !c || c.charges < 1)) throw new Error('Both cards must be in your hand.');
      if (cards.some(c => c.usedTurn === d.turn)) throw new Error('Used cards can be fused next turn. Fuse before casting.');
      const key = getRecipe(cards[0].key, cards[1].key);
      if (!key) throw new Error('Fuse two basic elements. Advanced spells are already complete.');
      d.hands[actor] = hand.filter(c => !action.cardIds.includes(c.id));
      const forged = makeCard(d, key);
      d.hands[actor].push(forged);
      emit(d, 'combine', `${SPELLS[key].name} forged`, -1, -1, { element: SPELLS[key].element, cardId: forged.id });
    } else if (action.type === 'gift') {
      if (action.to === actor || !d.players.includes(action.to)) throw new Error('Choose another party member.');
      if (d.hands[action.to].length >= MAX_HAND) throw new Error('That player’s hand is full.');
      const index = hand.findIndex(c => c.id === action.cardId);
      if (index < 0) throw new Error('That card is no longer in your hand.');
      const [card] = hand.splice(index, 1); d.hands[action.to].push(card);
      emit(d, 'gift', `${actor} gave ${SPELLS[card.key].name} to ${action.to}`, -1, -1, { element: SPELLS[card.key].element });
    } else if (action.type === 'endTurn') endTurn(d, actor);
    else throw new Error('Unknown game action.');
  }
  d.combos += d.events.filter(e => e.type === 'combo').length;
  d.revision = state.revision + 1;
  return d;
}
