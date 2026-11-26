import { HEROES, WORLD } from './data.mjs';
import { Game } from './engine.mjs';

const SIM_STEP = 1 / 30;
const MOVE_TIMEOUT = .7;
const TEAM_SIZE = 3;
const TEAMS = [0, 1];

// View snapshots carry the fields used by the existing battle renderer and HUD.
// Leave pathfinding, bot state, and enemy inventory/advancement out of the packet.
const UNIT_FIELDS = [
  'id', 'kind', 'team', 'x', 'y', 'r', 'hp', 'maxHp', 'alive', 'angle', 'moving', 'vx', 'vy',
  'attackAnim', 'attackTimer', 'status', 'shield', 'shieldTime', 'level', 'range', 'lane', 'tier',
  'name', 'displayName', 'heroId', 'slot', 'bot', 'subtype', 'scale', 'targetId', 'ownerId', 'deathTime',
  'siege', 'noVision', 'stationary', 'guardian', 'revealedUntil', 'respawnIn', 'recallIn',
  'visualAction', 'visualHit', 'visualDeath', 'visualSpawn', 'shieldVisual', 'shieldLayers',
  'magazine', 'resonance', 'channel', 'armAt', 'assembleUntil', 'overclockUntil', 'timeWard',
  'steadyAim', 'steadyAimUntil', 'streak',
];
const PLAYER_FIELDS = [
  'mana', 'maxMana', 'cooldowns', 'talents', 'kills', 'deaths', 'assists', 'gold', 'goldEarned',
  'items', 'lastGoldGain', 'metrics', 'evolution', 'xp', 'xpNext', 'lastDamageAt', 'damage',
  'attackRate', 'speed', 'baseHp', 'baseMana', 'expansion', 'cosmetics',
];
const SNAPSHOT_FIELDS = [
  'matchId', 'time', 'winner', 'stats', 'playerId', 'waveIn', 'waveNumber', 'beastIn', 'beastIndex',
  'units', 'projectiles', 'effects', 'texts', 'events', 'zones', 'scheduledImpacts', 'lastSeen',
  'attackOrder', 'pendingBeastChoice', 'moveTarget',
];

function fields(record, names) {
  const result = {};
  for (const name of names) if (record[name] !== undefined) result[name] = record[name];
  return result;
}

function mapPoint(point) {
  if (!Number.isFinite(point?.x) || !Number.isFinite(point?.y)) return null;
  return {
    x: Math.max(0, Math.min(WORLD.width, point.x)),
    y: Math.max(0, Math.min(WORLD.height, point.y)),
  };
}

function validPlayerId(id) {
  return typeof id === 'string' && id.length > 0 && id.length <= 128;
}

function isLane(lane) {
  return Number.isInteger(lane) && lane >= 0 && lane < TEAM_SIZE;
}

function displayName(name) {
  return typeof name === 'string' ? name.trim().slice(0, 28) : '';
}

export class OnlineMatch {
  constructor({roster, seed, matchId} = {}) {
    if (!Array.isArray(roster) || roster.length !== TEAMS.length || roster.some(team =>
      !Array.isArray(team) || team.length !== TEAM_SIZE || team.some(slot =>
        typeof slot?.hero !== 'string' || !Object.hasOwn(HEROES, slot.hero)))) {
      throw new TypeError('An online match needs three valid heroes per team.');
    }
    this.game = new Game({teams: roster.map(team => team.map(slot => slot.hero)), seed});
    this.matchId = matchId ?? globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.players = new Map();
    this.pending = 0;
    for (const team of TEAMS) for (const [slot, entry] of roster[team].entries()) {
      if (entry.playerId != null && !this.takeover(entry.playerId, team, slot, entry.name)) {
        throw new TypeError('Online player assignments must be unique.');
      }
    }
  }

  takeover(playerId, team, slot, name) {
    const game = this.game;
    if (!validPlayerId(playerId) || this.players.has(playerId) || !TEAMS.includes(team) || !isLane(slot) || game.winner !== null) return false;
    const hero = game.units.find(unit => unit.kind === 'hero' && unit.team === team && unit.slot === slot);
    if (!hero?.bot || !game.setHumanControl(hero, true)) return false;
    hero.displayName = displayName(name) || HEROES[hero.heroId].name;
    this.players.set(playerId, hero);
    return true;
  }

  release(playerId) {
    const hero = this.players.get(playerId);
    if (!hero) return false;
    this.game.setHumanControl(hero, false);
    delete hero.displayName;
    return this.players.delete(playerId);
  }

  advance(dt) {
    if (!Number.isFinite(dt) || dt <= 0) return;
    const game = this.game;
    this.pending += Math.min(dt, .25);
    while (this.pending + 1e-9 >= SIM_STEP && game.winner === null) {
      for (const control of game.humanControls.values()) {
        if (game.time >= control.moveUntil) {
          delete control.input.move;
          delete control.moveUntil;
        }
      }
      game.update(SIM_STEP);
      this.pending -= SIM_STEP;
    }
  }

  command(playerId, action) {
    const hero = this.players.get(playerId), game = this.game;
    if (!hero || !action || typeof action !== 'object' || game.winner !== null) return false;
    const control = game.controlFor(hero);
    switch (action.type) {
      case 'move': {
        if (!Number.isFinite(action.x) || !Number.isFinite(action.y)) return false;
        const aim = action.aim == null ? undefined : mapPoint(action.aim);
        if (aim === null) return false;
        const x = Math.max(-1, Math.min(1, action.x)), y = Math.max(-1, Math.min(1, action.y));
        const scale = Math.max(1, Math.hypot(x, y));
        control.input = {move: {x: x / scale, y: y / scale}, aim};
        control.moveUntil = game.time + MOVE_TIMEOUT;
        game.playerCommands(control.input, hero);
        return true;
      }
      case 'cancel':
        delete control.input.move;
        game.cancelPlayerOrder(hero);
        return true;
      case 'moveTo':
        return game.movePlayerTo(mapPoint(action), hero);
      case 'attack':
        return game.attackNearest(hero);
      case 'attackTarget':
        return Number.isSafeInteger(action.targetId) && game.attackTarget(action.targetId, hero);
      case 'cast': {
        const aim = mapPoint(action);
        return !!aim && Number.isInteger(action.slot) && action.slot >= 0 && action.slot < 4 && game.cast(hero, action.slot, aim);
      }
      case 'recall':
        return game.recall(hero);
      case 'talent':
        return Number.isInteger(action.level) && typeof action.id === 'string' && game.chooseTalent(hero, action.level, action.id);
      case 'buy':
        return typeof action.id === 'string' && game.buyItem(hero, action.id);
      case 'ping':
        return isLane(action.lane) && game.command(action.order, action.lane, hero);
      case 'beastLane':
        return Number.isSafeInteger(action.beastId) && game.chooseBeastLane(action.lane, action.beastId, hero);
      default:
        return false;
    }
  }

  snapshot(playerId) {
    const player = this.players.get(playerId), game = this.game;
    if (!player) return null;
    const team = player.team, control = game.controlFor(player), sources = game.visionSources(team);
    const pointVisible = ({x, y}) => sources.some(source => Math.hypot(x - source.x, y - source.y) <= source.r);
    const alive = new Set(game.units.filter(unit => unit.alive).map(unit => unit.id));
    const visible = new Set(game.units.filter(unit => game.canSee(team, unit)).map(unit => unit.id));
    const units = game.units.filter(unit => visible.has(unit.id) || unit.kind === 'core' || unit.kind === 'tower' ||
      unit.visualDeath?.visibleTo?.includes(team) && pointVisible(unit)).map(unit => ({
      ...fields(unit, UNIT_FIELDS),
      ...(unit === player ? {...fields(unit, PLAYER_FIELDS), isPlayer: true} : {}),
    }));
    const effects = game.effects.filter(effect => {
      if (game.winner !== null && effect.type === 'structure-collapse' && effect.kind === 'core') return true;
      if (effect.type === 'reward-gold' && effect.ownerId !== player.id) return false;
      if (!pointVisible(effect)) return false;
      const targetId = effect.visibilityId ?? effect.followId;
      return targetId == null || visible.has(targetId) || !alive.has(targetId) && effect.visibleTo?.includes(team);
    });
    const projectiles = game.projectiles.filter(pointVisible).map(({hit, ...shot}) => {
      if (shot.team !== team && !visible.has(shot.sourceId) && !pointVisible({x: shot.fromX, y: shot.fromY})) {
        shot.fromX = shot.x;
        shot.fromY = shot.y;
      }
      return shot;
    });
    const zones = game.zones.filter(zone => zone.team === team || pointVisible(zone)).map(({applied, ...zone}) => zone);
    const events = game.events.filter(event => (event.heroId == null || event.heroId === player.id) && (event.team == null || event.team === team));
    const lastSeen = [{}, {}];
    lastSeen[team] = game.lastSeen[team];
    return structuredClone({
      ...fields(game, ['time', 'winner', 'stats', 'waveIn', 'waveNumber', 'beastIn', 'beastIndex']),
      matchId: this.matchId, playerId: player.id, units, effects, projectiles, zones, events, lastSeen,
      texts: game.texts.filter(pointVisible),
      scheduledImpacts: game.scheduledImpacts.filter(impact => impact.team === team || pointVisible(impact)),
      attackOrder: control.attackOrder, moveTarget: control.moveTarget, combatCue: control.combatCue,
      pendingBeastChoice: game.beastChoice(team),
    });
  }
}

/** Install an authoritative frame on the existing Game-shaped renderer view. */
export function applyOnlineSnapshot(view, snapshot) {
  if (!view || !Number.isFinite(snapshot?.time) || !Array.isArray(snapshot.units)) return false;
  if (view.online && view.matchId === snapshot.matchId && view.time > snapshot.time) return false;
  const player = snapshot.units.find(unit => unit.id === snapshot.playerId && unit.kind === 'hero');
  if (!player || !Object.hasOwn(HEROES, player.heroId)) return false;
  const cue = snapshot.combatCue, previous = view.combatCue;
  Object.assign(view, fields(snapshot, SNAPSHOT_FIELDS), {player, online: true});
  if (cue?.at !== previous?.at || cue?.kind !== previous?.kind) view.combatCue = cue;
  return true;
}
