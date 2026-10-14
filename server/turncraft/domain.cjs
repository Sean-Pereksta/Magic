'use strict';
const FACTIONS = Object.freeze(['frontier', 'ascendant', 'brood']);
const MAPS = Object.freeze([
  'classic',
  'ember-rift',
  'frostline-basin',
  'verdant-divide',
  'shattered-dunes',
  'orbital-foundry'
]);
const MODES = Object.freeze(['annihilation', 'conquest', 'relic']);
const RANKS = Object.freeze([
  { name: 'Bronze', min: 0 },
  { name: 'Silver', min: 900 },
  { name: 'Gold', min: 1100 },
  { name: 'Platinum', min: 1300 },
  { name: 'Diamond', min: 1500 },
  { name: 'Master', min: 1750 }
]);
class TurncraftError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.code = code;
    this.status = status;
  }
}
function fail(ok, code, message, status) {
  if (!ok) throw new TurncraftError(code, message, status);
}
function plain(value) {
  return value && typeof value === 'object' && !Array.isArray(value);
}
function exact(data, keys) {
  fail(
    plain(data) && Object.keys(data).every((key) => keys.includes(key)),
    'INVALID_ARGUMENT',
    'Unsupported request fields.'
  );
}
function text(value, max = 80) {
  return typeof value === 'string' ? value.trim().slice(0, max) : '';
}
function id(value) {
  fail(
    typeof value === 'string' && /^[a-zA-Z0-9_-]{1,100}$/.test(value),
    'INVALID_ARGUMENT',
    'Invalid lobby or match code.'
  );
  return value;
}
function integer(value, min, max) {
  fail(
    Number.isInteger(value) && value >= min && value <= max,
    'INVALID_ARGUMENT',
    'A value is outside its supported range.'
  );
  return value;
}
function rankFor(mmr, matches = 0) {
  return matches > 0
    ? [...RANKS].reverse().find((tier) => mmr >= tier.min)?.name || 'Bronze'
    : 'Unranked';
}
function newProfile(who, stamp) {
  return {
    schemaVersion: 1,
    uid: who.uid,
    username: who.username || who.displayName || 'Commander',
    mmr: 1000,
    rank: 'Unranked',
    rankedWins: 0,
    rankedLosses: 0,
    rankedDraws: 0,
    rankedMatches: 0,
    provisionalMatchesRemaining: 10,
    highestMMR: 1000,
    lastRankedMatchAt: null,
    campaign: {
      highestCompletedMission: 0,
      unlockedRegions: [],
      completedMissions: {}
    },
    survival: { highestWave: 0, recordsByMap: {} },
    achievements: {},
    updatedAt: stamp
  };
}
function settings(data = {}) {
  exact(data, ['mapId', 'gameMode', 'maxPlayers', 'startingResources']);
  const result = {
    mapId: data.mapId || 'classic',
    gameMode: data.gameMode || 'annihilation',
    maxPlayers: data.maxPlayers || 3,
    startingResources: data.startingResources || 'standard'
  };
  fail(
    MAPS.includes(result.mapId) &&
      MODES.includes(result.gameMode) &&
      [2, 3].includes(result.maxPlayers) &&
      result.startingResources === 'standard',
    'INVALID_ARGUMENT',
    'Choose a supported battlefield and match setting.'
  );
  return result;
}
function elo(a, b, outcome, stamp) {
  fail(
    ['p0', 'p1', 'draw'].includes(outcome),
    'INVALID_RESULT',
    'The verified outcome is invalid.'
  );
  const expected = 1 / (1 + 10 ** ((b.mmr - a.mmr) / 400)),
    k = a.rankedMatches < 10 || b.rankedMatches < 10 ? 48 : 24,
    score = outcome === 'draw' ? 0.5 : outcome === 'p0' ? 1 : 0;
  const delta = Math.max(
    -a.mmr,
    Math.min(b.mmr, Math.round(k * (score - expected)))
  );
  return [a, b].map((profile, index) => {
    const d = delta === 0 ? 0 : index === 0 ? delta : -delta,
      won = outcome === `p${index}`,
      draw = outcome === 'draw',
      mmr = profile.mmr + d,
      rankedMatches = profile.rankedMatches + 1;
    return {
      ...profile,
      mmr,
      rank: rankFor(mmr, rankedMatches),
      rankedWins: profile.rankedWins + Number(won),
      rankedLosses: profile.rankedLosses + Number(!won && !draw),
      rankedDraws: profile.rankedDraws + Number(draw),
      rankedMatches,
      provisionalMatchesRemaining: Math.max(0, 10 - rankedMatches),
      highestMMR: Math.max(profile.highestMMR, mmr),
      lastRankedMatchAt: stamp,
      updatedAt: stamp,
      lastDelta: d
    };
  });
}
module.exports = {
  FACTIONS,
  MAPS,
  MODES,
  RANKS,
  TurncraftError,
  fail,
  plain,
  exact,
  text,
  id,
  integer,
  rankFor,
  newProfile,
  settings,
  elo
};
