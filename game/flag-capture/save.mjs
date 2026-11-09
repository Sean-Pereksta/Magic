import { FACTIONS, MAPS, MODES, DIFFICULTIES, DEFAULT_ARMY, DRAFT_ROLES, GROUND_EFFECTS, ORDER_CAP, ORDER_INTERVAL, armyCost } from './engine.mjs';
import { CAMPAIGN } from './campaign.mjs';

const SIDES = ['player', 'ai'];
const TERRAIN = new Set(['grass', 'rock', 'grove', 'water']);
const DENSITIES = new Set(['sparse', 'balanced', 'wild']);
const CAMPAIGN_IDS = new Set(CAMPAIGN.map(level => level.id));
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (value, key) => record(value) && typeof key === 'string' && Object.hasOwn(value, key);
const finite = (value, min = 0, max = Number.MAX_SAFE_INTEGER) => Number.isFinite(value) && value >= min && value <= max;
const integer = (value, min = 0, max = Number.MAX_SAFE_INTEGER) => Number.isSafeInteger(value) && value >= min && value <= max;
const coordinate = (game, value) => record(value) && integer(value.x, 0, game.width - 1) && integer(value.y, 0, game.height - 1);
const position = value => `${value.x},${value.y}`;

/** Read untrusted device storage without changing the supplied object. */
export function parseBattleSave(value) {
  let save;
  try {
    save = typeof value === 'string' ? JSON.parse(value) : structuredClone(value);
  } catch {
    return null;
  }
  if (!record(save) || save.version !== 2 || !record(save.game)) return null;
  const game = save.game;
  if (![7, 9, 11].includes(game.width) || ![5, 7, 9].includes(game.height)) return null;
  if (!own(MAPS, game.map) || !record(game.factions) || !SIDES.every(side => own(FACTIONS, game.factions[side]))) return null;
  if (game.mode === undefined) game.mode = 'beacons';
  if (game.terrain === undefined) game.terrain = 'balanced';
  if (game.opponentMode === undefined) game.opponentMode = 'ai';
  if (game.startingMoney === undefined) game.startingMoney = 24;
  if (game.groundEffects === undefined) game.groundEffects = [];
  if (game.nextGroundId === undefined) game.nextGroundId = 0;
  if (game.armies === undefined) game.armies = {player:[...DEFAULT_ARMY],ai:[...DEFAULT_ARMY]};
  if (!own(MODES, game.mode) || !DENSITIES.has(game.terrain) || !own(DIFFICULTIES, game.difficulty)) return null;
  if (!['ai', 'local'].includes(game.opponentMode)) return null;
  if (!integer(game.startingMoney, 12, 40) || !record(game.armies)) return null;
  for (const side of SIDES) {
    const army = game.armies[side];
    if (!Array.isArray(army) || army.length < 1 || army.length > 8 || !army.every(role => DRAFT_ROLES.includes(role)) || army.filter(role => role === 'leader').length !== 1 || armyCost(army) > game.startingMoney) return null;
  }
  if (game.deployments !== undefined) {
    if (!record(game.deployments)) return null;
    for (const side of SIDES) {
      const deployment = game.deployments[side];
      if (!Array.isArray(deployment) || deployment.length !== game.armies[side].length) return null;
      const positions = new Set();
      for (const point of deployment) {
        if (!coordinate(game, point) || (side === 'player' ? point.y < game.height - 2 : point.y > 1) || positions.has(position(point))) return null;
        positions.add(position(point));
      }
    }
  }
  if (!finite(game.duration, 60, 300) || !finite(game.time, 0, game.duration)) return null;
  if (game.orders === undefined) game.orders = {player:1, ai:1};
  if (game.nextOrderAt === undefined) game.nextOrderAt = game.time + ORDER_INTERVAL;
  if (game.telegraphs === undefined) game.telegraphs = [];
  if (!record(game.orders) || !SIDES.every(side => integer(game.orders[side], 0, ORDER_CAP))) return null;
  if (!finite(game.nextOrderAt, 0, game.time + ORDER_INTERVAL + 1e-6)) return null;
  if (game.mode === 'elimination' ? game.goal !== 0 : !integer(game.goal, 1, 100)) return null;
  if (!SIDES.includes(game.activeSide) || ![null, 'player', 'ai', 'draw'].includes(game.winner)) return null;
  if (game.campaignId !== undefined && !CAMPAIGN_IDS.has(game.campaignId)) return null;
  if (!record(game.scores) || !record(game.supplies) || !SIDES.every(side => integer(game.scores[side]) && integer(game.supplies[side], 0, 40))) return null;
  if (!['nextPulseAt', 'nextAiAt', 'aiRecruitAt'].every(key => finite(game[key]))) return null;
  if (!integer(game.nextId) || !integer(game.rng, 0, 0xffffffff)) return null;
  if (!Array.isArray(game.log) || game.log.length > 80 || !game.log.every(entry => typeof entry === 'string' && entry.length <= 1000)) return null;

  if (!Array.isArray(game.tiles) || game.tiles.length !== game.width * game.height) return null;
  if (!game.tiles.every((tile, index) => coordinate(game, tile) && tile.x === index % game.width && tile.y === Math.floor(index / game.width) && TERRAIN.has(tile.terrain))) return null;
  const passable = (value, movement = 'Ground') => {
    const terrain = game.tiles[value.y * game.width + value.x].terrain;
    return terrain !== 'rock' && (terrain !== 'water' || movement === 'Flight');
  };
  const flagCount = game.campaignId === 'first-light' && game.mode === 'control' ? 1 : 3;
  if (!Array.isArray(game.flags) || game.flags.length !== flagCount) return null;
  const flagPositions = new Set();
  for (const flag of game.flags) {
    if (!coordinate(game, flag) || !passable(flag) || ![null, ...SIDES].includes(flag.owner)) return null;
    if (typeof flag.name !== 'string' || !/^[A-Za-z][A-Za-z0-9 '\-]{0,39}$/.test(flag.name)) return null;
    if (flag.contested !== undefined && typeof flag.contested !== 'boolean') return null;
    if (flagPositions.has(position(flag))) return null;
    flagPositions.add(position(flag));
  }

  if (!Array.isArray(game.units) || game.units.length > 200) return null;
  const ids = new Set(), occupied = new Set(), counts = { player: 0, ai: 0 };
  for (const unit of game.units) {
    if (!coordinate(game, unit) || !SIDES.includes(unit.side) || unit.faction !== game.factions[unit.side] || !own(FACTIONS[unit.faction].units, unit.role)) return null;
    if (typeof unit.id !== 'string' || !/^u[1-9]\d*$/.test(unit.id) || ids.has(unit.id)) return null;
    const numericId = Number(unit.id.slice(1));
    if (!integer(numericId, 1, game.nextId)) return null;
    ids.add(unit.id);
    const definition = FACTIONS[unit.faction].units[unit.role];
    if (unit.maxHp !== definition.hp || !finite(unit.hp, 0, unit.maxHp)) return null;
    if (unit.braceUntil === undefined) unit.braceUntil = 0;
    if (!finite(unit.braceUntil, 0, game.time + 3 + 1e-6)) return null;
    if (!['readyAt', 'abilityReadyAt', 'rootedUntil'].every(key => finite(unit[key]))) return null;
    const abilities = definition.abilities || (definition.ability ? [definition.ability] : []);
    if (unit.abilityReadyAts === undefined) unit.abilityReadyAts = abilities.map((_, index) => index === 0 ? unit.abilityReadyAt : 0);
    // Older two-special creatures receive their newly added summon ready to use.
    if (Array.isArray(unit.abilityReadyAts) && unit.abilityReadyAts.length === 2 && abilities.length === 3) unit.abilityReadyAts.push(0);
    if (!Array.isArray(unit.abilityReadyAts) || unit.abilityReadyAts.length !== abilities.length || !unit.abilityReadyAts.every(readyAt => finite(readyAt))) return null;
    if (abilities.length ? unit.abilityReadyAts[0] !== unit.abilityReadyAt : unit.abilityReadyAt !== 0) return null;
    if (!integer(unit.shield, 0, 5) || !integer(unit.cursed, 0, 1)) return null;
    if (unit.hp > 0) {
      if (!passable(unit, definition.movement) || occupied.has(position(unit)) || ++counts[unit.side] > 8) return null;
      occupied.add(position(unit));
    }
  }
  if (!Array.isArray(game.telegraphs) || game.telegraphs.length > 16) return null;
  const charging = new Set();
  for (const aim of game.telegraphs) {
    if (!coordinate(game, aim) || aim.kind !== 'foldhorn' || charging.has(aim.unitId)) return null;
    const ram = game.units.find(unit => unit.id === aim.unitId);
    if (!ram || ram.hp <= 0 || ram.side !== aim.side || ram.faction !== 'chaos' || ram.role !== 'brute') return null;
    if (ram.x !== aim.fromX || ram.y !== aim.fromY || !finite(aim.resolvesAt, game.time, game.time + 3 + 1e-6)) return null;
    const dx = Math.abs(aim.x - ram.x), dy = Math.abs(aim.y - ram.y);
    if ((dx && dy) || dx + dy < 1 || dx + dy > 3) return null;
    charging.add(ram.id);
  }
  if (!integer(game.nextGroundId) || !Array.isArray(game.groundEffects) || game.groundEffects.length > game.width*game.height*2) return null;
  const effectIds=new Set(),paintedCells=new Set();
  for(const effect of game.groundEffects){
    if(!coordinate(game,effect)||!own(GROUND_EFFECTS,effect.kind)||!SIDES.includes(effect.side)||!passable(effect))return null;
    if(typeof effect.id!=='string'||!/^g[1-9]\d*$/.test(effect.id)||effectIds.has(effect.id)||!integer(Number(effect.id.slice(1)),1,game.nextGroundId))return null;
    const source=game.units.find(unit=>unit.id===effect.sourceId),cell=`${effect.side}:${position(effect)}`;
    if(!source||source.side!==effect.side||paintedCells.has(cell))return null;
    const allowed=Object.values(FACTIONS[source.faction].units).some(def=>def.abilities.some(skill=>skill.groundKind===effect.kind));
    if(!allowed)return null;
    if(!finite(effect.createdAt,0,game.time+1e-7)||!finite(effect.expiresAt,effect.createdAt)||effect.expiresAt<=effect.createdAt||effect.expiresAt>effect.createdAt+GROUND_EFFECTS[effect.kind].duration+1e-7)return null;
    if(!finite(effect.nextTickAt,effect.createdAt,effect.expiresAt+3+1e-7))return null;
    effectIds.add(effect.id);paintedCells.add(cell);
  }
  return { version: 2, game };
}
