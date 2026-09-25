import { initializeFog } from '../../fog.mjs';
import { initializeCooperation } from '../../cooperation-state.mjs';
// Pre-regional fixture for legacy campaign gameplay/migration regression coverage.
import { HOUSES, SAVE_VERSION } from '../../data.mjs';
import { tileId, random, neighbors, passable, distance, rebuildTerritory, log } from '../../core.mjs';
import { regionalize, emptyUnits } from '../../economy.mjs';
import { initializeStrategy } from '../../strategy.mjs';
import { initializeLiving } from '../../living.mjs';
import { initializePlans } from '../../plans.mjs';
import { initializeEspionage } from '../../espionage.mjs';
import { updateAttitudes } from '../../politics.mjs';
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
    const k = { ...house, resources: { food: 140, wood: 110, stone: 90, iron: 50, gold: 200, horses: 12, tools: 0, arms: 0 }, population: 80, happiness: 70, tax: 'medium', commands: 3, relations: {}, memories: [], memorySummary: '', goal: 'ECONOMY', lastGiftTurn: -1 };
    for (const other of HOUSES) if (other.id !== k.id) k.relations[other.id] = { opinion: k.honor > .8 ? 18 : k.aggression > .8 ? -12 : 5, trust: 15 };
    s.kingdoms.push(k);
    const [q, r] = starts[i], capital = s.tiles[tileId(q, r)];
    Object.assign(capital, { terrain: 'plains', building: 'city', owner: k.id, capital: k.id, road: true, resource: null, name: ['Emberkeep', 'Frostwatch', 'Briarhold', 'Solstice', 'Moonveil', 'Redhaven'][i] });
    const ring = neighbors(s, capital);
    ring.forEach((t, j) => Object.assign(t, { terrain: ['plains', 'forest', 'hills', 'hills', 'plains', 'plains'][j], resource: ['food', 'wood', 'stone', 'iron', 'food', null][j], building: ['farm', 'lumber', 'quarry', 'mine', null, null][j], road: true, owner: k.id }));
    s.armies.push({ id: `army-${s.nextId++}`, owner: k.id, tile: capital.id, units: {...emptyUnits(), levy: 20, archer: 6, cavalry: 2}, formation: 'balanced', retreats: 0, morale: 1, order: 'hold', path: [], target: null });
  });
  regionalize(s, random);
  s.commerce = {offers:[],lastOfferTurn:0,cooldowns:{},aiTrades:{}};
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
  initializeStrategy(s);
  initializeFog(s); initializeLiving(s); initializePlans(s); initializeCooperation(s); initializeEspionage(s); updateAttitudes(s);
  return s;
}
